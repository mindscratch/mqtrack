package foobarbaz

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	// UDP packet limit, see
	// https://en.wikipedia.org/wiki/User_Datagram_Protocol#Packet_structure
	UDP_MAX_PACKET_SIZE int = 64 * 1024

	defaultFieldName = "value"

	defaultProtocol = "udp"

	defaultSeparator           = "_"
	defaultAllowPendingMessage = 10000
	MaxTCPConnections          = 250
)

var dropwarn = "E! Error: server message queue full. " +
	"We have dropped %d messages so far. " +
	"You may want to increase allowed_pending_messages in the config\n"

var malformedwarn = "E! server over TCP has received %d malformed packets" +
	" thus far."

// Initially this code was copied from https://github.com/influxdata/telegraf/blob/master/plugins/inputs/statsd/statsd.go
type Server struct {
	// Protocol used on listener - udp or tcp
	Protocol string `toml:"protocol"`

	// Address & Port to serve from
	ServiceAddress string

	// Number of messages allowed to queue up in between calls to Gather. If this
	// fills up, packets will get dropped until the next Gather interval is ran.
	AllowedPendingMessages int

	// Percentiles specifies the percentiles that will be calculated for timing
	// and histogram stats.
	Percentiles     []int
	PercentileLimit int

	DeleteGauges   bool
	DeleteCounters bool
	DeleteSets     bool
	DeleteTimings  bool
	ConvertNames   bool

	LineParser *regexp.Regexp

	// UDPPacketSize is deprecated, it's only here for legacy support
	// we now always create 1 max size buffer and then copy only what we need
	// into the in channel
	// see https://github.com/influxdata/telegraf/pull/992
	UDPPacketSize int `toml:"udp_packet_size"`

	ReadBufferSize int `toml:"read_buffer_size"`

	sync.Mutex
	// Lock for preventing a data race during resource cleanup
	cleanup sync.Mutex
	wg      sync.WaitGroup
	// accept channel tracks how many active connections there are, if there
	// is an available bool in accept, then we are below the maximum and can
	// accept the connection
	accept chan bool
	// drops tracks the number of dropped metrics.
	drops int
	// malformed tracks the number of malformed packets
	malformed int

	// Channel for all incoming statsd packets
	in   chan *bytes.Buffer
	done chan struct{}

	// bucket -> influx templates
	Templates []string

	// Protocol listeners
	UDPlistener *net.UDPConn
	TCPlistener *net.TCPListener

	// track current connections so we can close them in Stop()
	conns map[string]*net.TCPConn

	MaxTCPConnections int `toml:"max_tcp_connections"`

	TCPKeepAlive       bool           `toml:"tcp_keep_alive"`
	TCPKeepAlivePeriod *time.Duration `toml:"tcp_keep_alive_period"`

	// A pool of byte slices to handle parsing
	bufPool sync.Pool
}

func (s *Server) Start() error {
	s.Lock()
	defer s.Unlock()

	s.in = make(chan *bytes.Buffer, s.AllowedPendingMessages)
	s.done = make(chan struct{})
	s.accept = make(chan bool, s.MaxTCPConnections)
	s.conns = make(map[string]*net.TCPConn)
	s.bufPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	for i := 0; i < s.MaxTCPConnections; i++ {
		s.accept <- true
	}

	s.wg.Add(2)
	// Start the UDP listener
	if s.isUDP() {
		go s.udpListen()
	} else {
		go s.tcpListen()
	}
	// Start the line parser
	go s.parser()
	log.Printf("I! Started the statsd service on %s\n", s.ServiceAddress)
	return nil
}

func (s *Server) Wait() {
	s.wg.Wait()
}

// tcpListen() starts listening for tcp packets on the configured port.
func (s *Server) tcpListen() error {
	defer s.wg.Done()
	// Start listener
	var err error
	address, _ := net.ResolveTCPAddr("tcp", s.ServiceAddress)
	s.TCPlistener, err = net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenTCP - %s", err)
		return err
	}
	log.Println("I! TCP Server listening on: ", s.TCPlistener.Addr().String())
	for {
		select {
		case <-s.done:
			return nil
		default:
			// Accept connection:
			conn, err := s.TCPlistener.AcceptTCP()
			if err != nil {
				return err
			}

			if s.TCPKeepAlive {
				if err = conn.SetKeepAlive(true); err != nil {
					return err
				}

				if s.TCPKeepAlivePeriod != nil {
					if err = conn.SetKeepAlivePeriod(*s.TCPKeepAlivePeriod); err != nil {
						return err
					}
				}
			}

			select {
			case <-s.accept:
				// not over connection limit, handle the connection properly.
				s.wg.Add(1)
				// generate a random id for this TCPConn
				id := RandomString(6)
				s.remember(id, conn)
				go s.handler(conn, id)
			default:
				// We are over the connection limit, refuse & close.
				s.refuser(conn)
			}
		}
	}
}

// udpListen starts listening for udp packets on the configured port.
func (s *Server) udpListen() error {
	defer s.wg.Done()
	var err error
	address, _ := net.ResolveUDPAddr(s.Protocol, s.ServiceAddress)
	s.UDPlistener, err = net.ListenUDP(s.Protocol, address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	log.Println("I! Server UDP listener listening on: ", s.UDPlistener.LocalAddr().String())

	if s.ReadBufferSize > 0 {
		s.UDPlistener.SetReadBuffer(s.ReadBufferSize)
	}

	buf := make([]byte, UDP_MAX_PACKET_SIZE)
	for {
		select {
		case <-s.done:
			return nil
		default:
			n, _, err := s.UDPlistener.ReadFromUDP(buf)
			if err != nil && !strings.Contains(err.Error(), "closed network") {
				log.Printf("E! Error READ: %s\n", err.Error())
				continue
			}
			b := s.bufPool.Get().(*bytes.Buffer)
			b.Reset()
			b.Write(buf[:n])

			select {
			case s.in <- b:
			default:
				s.drops++
				if s.drops == 1 || s.AllowedPendingMessages == 0 || s.drops%s.AllowedPendingMessages == 0 {
					log.Printf(dropwarn, s.drops)
				}
			}
		}
	}
}

// parser monitors the s.in channel, if there is a packet ready, it parses the
// packet into strings and then calls parseLine, which parses a
// single record into a struct.
func (s *Server) parser() error {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return nil
		case buf := <-s.in:
			lines := strings.Split(buf.String(), "\n")
			s.bufPool.Put(buf)
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					// TODO
					fmt.Println("LINE:", line)
					s.parseLine(line)
				}
			}
		}
	}
}

// parseLine will parse the given line, validating it as it goes.
// If the line is valid, it will be stored
func (s *Server) parseLine(line string) error {
	s.Lock()
	defer s.Unlock()

	// Validate splitting the line
	bits := s.LineParser.FindStringSubmatch(line)
	if len(bits) < 4 {
		log.Printf("E! Error: splitting, got %d bits, line: %v\n", len(bits), bits)
		return errors.New("Error Parsing statsd line")
	}

	// Extract bucket name from individual metric bits
	appName, dests, publisherOrConsumer := bits[1], bits[2], bits[3]
	fmt.Println("app", appName, "dests", dests)
	destinations := strings.Split(dests, ",")
	record := NewRecord(appName, destinations, publisherOrConsumer == "p", publisherOrConsumer == "c")
	fmt.Printf("record: %#v\n", record)
	// Save(record, s.db)

	return nil
}

// handler handles a single TCP Connection
func (s *Server) handler(conn *net.TCPConn, id string) {
	// connection cleanup function
	defer func() {
		s.wg.Done()
		conn.Close()
		// Add one connection potential back to channel when this one closes
		s.accept <- true
		s.forget(id)
	}()

	var n int
	scanner := bufio.NewScanner(conn)
	for {
		select {
		case <-s.done:
			return
		default:
			if !scanner.Scan() {
				return
			}
			n = len(scanner.Bytes())
			if n == 0 {
				continue
			}

			b := s.bufPool.Get().(*bytes.Buffer)
			b.Reset()
			b.Write(scanner.Bytes())
			b.WriteByte('\n')

			select {
			case s.in <- b:
			default:
				s.drops++
				if s.drops == 1 || s.drops%s.AllowedPendingMessages == 0 {
					log.Printf(dropwarn, s.drops)
				}
			}
		}
	}
}

// refuser refuses a TCP connection
func (s *Server) refuser(conn *net.TCPConn) {
	conn.Close()
	log.Printf("I! Refused TCP Connection from %s", conn.RemoteAddr())
	log.Printf("I! WARNING: Maximum TCP Connections reached, you may want to" +
		" adjust max_tcp_connections")
}

// forget a TCP connection
func (s *Server) forget(id string) {
	s.cleanup.Lock()
	defer s.cleanup.Unlock()
	delete(s.conns, id)
}

// remember a TCP connection
func (s *Server) remember(id string, conn *net.TCPConn) {
	s.cleanup.Lock()
	defer s.cleanup.Unlock()
	s.conns[id] = conn
}

func (s *Server) Stop() {
	s.Lock()
	log.Println("I! Stopping the statsd service")
	close(s.done)
	if s.isUDP() {
		s.UDPlistener.Close()
	} else {
		s.TCPlistener.Close()
		// Close all open TCP connections
		//  - get all conns from the s.conns map and put into slice
		//  - this is so the forget() function doesnt conflict with looping
		//    over the s.conns map
		var conns []*net.TCPConn
		s.cleanup.Lock()
		for _, conn := range s.conns {
			conns = append(conns, conn)
		}
		s.cleanup.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	}
	s.Unlock()

	s.wg.Wait()

	s.Lock()
	close(s.in)
	log.Println("I! Stopped Server listener service on ", s.ServiceAddress)
	s.Unlock()
}

// IsUDP returns true if the protocol is UDP, false otherwise.
func (s *Server) isUDP() bool {
	return strings.HasPrefix(s.Protocol, "udp")
}

// RandomString returns a random string of alpha-numeric characters
const alphanum string = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func RandomString(n int) string {
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}
