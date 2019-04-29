package foobarbaz

// One statsd metric, form is <bucket>:<value>|<mtype>|@<samplerate>
type Record struct {
	AppName      string
	Destinations []string
	Publisher    bool
	Consumer     bool
}

func NewRecord(appName string, destinations []string, publisher, consumer bool) *Record {
	r := Record{
		AppName:      appName,
		Destinations: destinations,
		Publisher:    publisher,
		Consumer:     consumer,
	}
	return &r
}
