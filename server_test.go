package foobarbaz

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_InvokesLineParser(t *testing.T) {
	testRecord := &Record{
		AppName:      "myapp",
		Destinations: []string{"a", "b", "c"},
		Publisher:    true,
	}

	srv := NewServer()
	lineParserInvoked := false
	srv.LineParser = func(line string) (*Record, error) {
		lineParserInvoked = true
		return testRecord, nil
	}

	err := srv.Start()
	require.Nil(t, err)
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.ServiceAddress)
	require.Nil(t, err)
	defer conn.Close()

	conn.Write([]byte(fmt.Sprintf("%s|%s|%s\n", testRecord.AppName, strings.Join(testRecord.Destinations, ","), "p")))
	time.Sleep(10 * time.Millisecond)
	assert.True(t, lineParserInvoked, "Expected line parser to be invoked")
}
