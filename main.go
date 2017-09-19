package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/nickethier/flowjo/consumer"
)

type options struct {
	Broker string
	Topic  string
	Debug  bool
}

var opts options

func init() {
	flag.StringVar(&opts.Broker, "broker", "127.0.0.1:9092", "broker ipaddress:port")
	flag.StringVar(&opts.Topic, "topic", "vflow.ipfix", "kafka topic")
	flag.BoolVar(&opts.Debug, "debug", false, "enabled/disabled debug")

	flag.Parse()
}

/*func flowID(rec record) string {
    return fmt.Sprintf("%s:%d-%s:%d",
}*/

func main() {

	consumer, err := consumer.NewConsumer(consumer.Config{
		Broker: []string{opts.Broker},
		Topic:  []string{opts.Topic},
		Debug:  opts.Debug,
		Group:  "mygroup",
	})

	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	recChan := consumer.Start()

	w := new(tabwriter.Writer)

	// Format in tab-separated columns with a tab stop of 8.
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	tickChan := time.NewTicker(time.Millisecond * 500).C
	for {
		select {
		case record := <-recChan:
			fmt.Fprintf(w, "%s -\t(AS%d)  %s:%d\t->\t(AS%d)  %s:%d\t%d\n", record.AgentID, record.SourceAS, record.SourceAddress, record.SourcePort, record.DestAS, record.DestAddress, record.DestPort, record.OctetDelta)

		case <-tickChan:
			w.Flush()
		}
	}
}
