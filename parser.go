package foobarbaz

import (
	"errors"
	"regexp"
	"strings"
)

// LineParserFunc is a function invoke for each line processed
// by the server.
type LineParserFunc func(line string) (*Record, error)

// DefaultLineParser provides the default LineParserFunc
func DefaultLineParser() LineParserFunc {
	// <app name>|destinations comma-delimited|<p or c for publisher or consumer>
	lp := regexp.MustCompile(`^([^|]+)\|(.*)\|([pc])$`)

	return func(line string) (*Record, error) {
		// Validate splitting the line
		bits := lp.FindStringSubmatch(line)
		if len(bits) < 4 {
			return nil, errors.New("expected 3 parts: <app name>|<destinations comma-delimited>|<p or c for publisher or consumer>")
		}

		// extract the parts
		appName, dests, publisherOrConsumer := bits[1], bits[2], bits[3]
		destinations := strings.Split(dests, ",")
		record := NewRecord(appName, destinations, publisherOrConsumer == "p", publisherOrConsumer == "c")
		return record, nil
	}
}
