package mqtrack

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// NotConnectedError represents an operation performed when the client is not connected.
type NotConnectedError struct {
	message string
}

// verify, at compile-time, that NotConnectedError implements the error interface
var _ error = &NotConnectedError{}

// NewNotConnectedError creates a new NotConnectedError
func NewNotConnectedError(message string) *NotConnectedError {
	e := NotConnectedError{message: message}
	return &e
}

// Error returns the error message.
func (e *NotConnectedError) Error() string {
	return e.message
}

// Client is a client which uses UDP for recording MQ tracking messages.
type Client struct {
	addr    *net.UDPAddr
	conn    *net.UDPConn
	appName string
}

// NewClient creates a new client and resolves the given address, returning an
// error if there's a problem with the address.
//
// The appName is the name of the application being tracked.
func NewClient(address, appName string) (*Client, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	if appName == "" {
		return nil, fmt.Errorf("appName must be provided")
	}

	c := Client{
		addr:    addr,
		appName: appName,
	}
	return &c, nil
}

// SetAppName configures the application name being tracked
func (c *Client) SetAppName(appName string) {
	c.appName = appName
}

// Connect "connects" to the UDP address.
//
// From https://ops.tips/blog/udp-client-and-server-in-go/#sending-udp-packets-using-go:
// 	Although we're not in a connection-oriented transport,
// 	the act of `dialing` is analogous to the act of performing
// 	a `connect(2)` syscall for a socket of type SOCK_DGRAM:
// 	- it forces the underlying socket to only read and write
//    to and from a specific remote address.
func (c *Client) Connect() error {
	conn, err := net.DialUDP("udp", nil, c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Record that the application published (or consumed, if publisher is false)
// to the given destinations. If the client is not connected a
// NotConnectedError is returned. If a problem occurs while sending,
// another error will be returned.
func (c *Client) Record(destinations []string, publisher bool) error {
	if c.conn == nil {
		return NewNotConnectedError(fmt.Sprintf("client is no longer connected to %s", c.addr.String()))
	}
	if len(destinations) == 0 {
		return errors.New("unable to record, destinations were not provided")
	}
	mode := "c"
	if publisher {
		mode = "p"
	}

	_, err := fmt.Fprintf(c.conn, c.appName+"|"+strings.Join(destinations, ",")+"|"+mode)
	return err
}
