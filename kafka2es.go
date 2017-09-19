// https://raw.githubusercontent.com/mattbaird/elastigo/master/tutorial/start_1.go
// go get github.com/mattbaird/elastigo
//
package main

import (
	"flag"
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/nickethier/flowjo/consumer"
	"log"
	"math/rand"
	"os"
	"text/tabwriter"
	"time"
)

var (
	host *string = flag.String("host", "localhost", "Elasticsearch Host")
)

type options struct {
	Broker string
	Topic  string
	Debug  bool
}

var opts options

func init() {
	//10.20.150.44
	//172.16.192.204
	flag.StringVar(&opts.Broker, "broker", "172.16.192.204:9092", "broker ipaddress:port")
	flag.StringVar(&opts.Topic, "topic", "vflow.ipfix", "kafka topic")
	flag.BoolVar(&opts.Debug, "debug", false, "enabled/disabled debug")

	flag.Parse()
}

/*func flowID(rec record) string {
    return fmt.Sprintf("%s:%d-%s:%d",
}*/

func main() {

	c := elastigo.NewConn()
	log.SetFlags(log.LstdFlags)
	flag.Parse()

	// Trace all requests
	c.RequestTracer = func(method, url, body string) {
		log.Printf("Requesting %s %s", method, url)
		log.Printf("Request body: %s", body)
	}

	fmt.Println("host = ", *host)
	// Set the Elasticsearch Host to Connect to
	c.Domain = *host

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
			session_time := record.EndTime - record.StartTime
			rand.Seed(time.Now().UTC().UnixNano())
			dd := fmt.Sprintf("{\"flow_time\": \"%d\",\"session_durration\":\"%d\",\"ip_addr_asr\":\"%s\",\"src_as\":\"AS%d\",\"src_ip\":\"%s\",\"src_port\":\"%d\",\"dst_as\":\"AS%d\",\"dst_ip\":\"%s\",\"dst_port\":\"%d\",\"count\":\"%d\"}\n", record.ExportTime, session_time, record.AgentID, record.SourceAS, record.SourceAddress, record.SourcePort, record.DestAS, record.DestAddress, record.DestPort, record.OctetDelta)

			//ExportTime
			//StartTime
			//EndTime

			_, err = c.Index("ipfix", "flows", nil, nil, dd)
			fmt.Fprintf(w, dd)
			exitIfErr(err)

		case <-tickChan:
			w.Flush()
		}
	}
}

func exitIfErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(1)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
