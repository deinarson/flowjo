package consumer

import (
	"encoding/json"
	"net"
	"strconv"
	"time"

	cluster "github.com/bsm/sarama-cluster"
)

type IPFIXField int

const (
	IPFIXFieldReserved IPFIXField = iota
	IPFIXFieldOctetDelta
	IPFIXFieldPacketDelta
	IPFIXFieldDeltaFlowCount
	IPFIXFieldProto
	IPFIXFieldIPClass
	IPFIXFieldTCPControlBitset
	IPFIXFieldSourcePort
	IPFIXFieldSourceIPv4Address
	IPFIXFieldSourceIPv4PrefixLength
	IPFIXFieldIngressInterface
	IPFIXFieldDestPort
	IPFIXFieldDestIPv4Address
	IPFIXFieldDestIPv4PrefixLength
	IPFIXFieldEgressInterface
	IPFIXFieldIPNextHopIPv4Address
	IPFIXFieldSourceAS
	IPFIXFieldDestAS
	IPFIXField18
	IPFIXField19
	IPFIXField20
	IPFIXFIeldFlowEndTimestamp
	IPFIXFIeldFlowStartTimestamp
)

type Config struct {
	Broker []string
	Topic  []string
	Debug  bool
	Group  string
}

type dataField struct {
	I IPFIXField
	V interface{}
}

type ipfix struct {
	AgentID  string
	DataSets [][]dataField
	Header   flowHeader
}

type flowHeader struct {
	Version    int64
	Length     int64
	ExportTime int64
	SequenceNo int64
	DomainID   int64
}

type Consumer struct {
	config        Config
	clusterConfig *cluster.Config
	consumer      *cluster.Consumer
}

func NewConsumer(conf Config) (*Consumer, error) {
	c := &Consumer{config: conf}
	c.clusterConfig = cluster.NewConfig()
	c.clusterConfig.Consumer.Return.Errors = true
	c.clusterConfig.Group.Return.Notifications = true
	consumer, err := cluster.NewConsumer(c.config.Broker, c.config.Group, c.config.Topic, c.clusterConfig)
	if err != nil {
		return nil, err
	}
	c.consumer = consumer

	return c, nil
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}

func (c *Consumer) Start() chan *FlowRecord {
	recChan := make(chan *FlowRecord)
	go func() {
		var objmap ipfix
		for {
			select {
			case msg, more := <-c.consumer.Messages():
				if more {
					if err := json.Unmarshal(msg.Value, &objmap); err == nil {
						for _, data := range objmap.DataSets {
							rec := &FlowRecord{
								AgentID:    objmap.AgentID,
								ExportTime: time.Unix(objmap.Header.ExportTime, 0),
							}
							for _, dd := range data {
								switch dd.I {
								case IPFIXFieldOctetDelta:
									if i, err := strconv.ParseUint(dd.V.(string), 0, 64); err == nil {
										rec.OctetDelta = i
									}
								case IPFIXFieldPacketDelta:
									if i, err := strconv.ParseUint(dd.V.(string), 0, 64); err == nil {
										rec.PacketDelta = i
									}
								case IPFIXFieldProto:
								case IPFIXFieldIPClass:
								case IPFIXFieldTCPControlBitset:
								case IPFIXFieldSourcePort:
									rec.SourcePort = int(dd.V.(float64))
								case IPFIXFieldSourceIPv4Address:
									rec.SourceAddress = net.ParseIP(dd.V.(string))
								case IPFIXFieldSourceIPv4PrefixLength:
									rec.SourcePrefixLength = int(dd.V.(float64))
								case IPFIXFieldIngressInterface:
									rec.IngressInterface = int(dd.V.(float64))
								case IPFIXFieldDestPort:
									rec.DestPort = int(dd.V.(float64))
								case IPFIXFieldDestIPv4Address:
									rec.DestAddress = net.ParseIP(dd.V.(string))
								case IPFIXFieldDestIPv4PrefixLength:
									rec.DestPrefixLength = int(dd.V.(float64))
								case IPFIXFieldEgressInterface:
									rec.DestPrefixLength = int(dd.V.(float64))
								case IPFIXFieldIPNextHopIPv4Address:
									rec.NextHopAddress = net.ParseIP(dd.V.(string))
								case IPFIXFieldSourceAS:
									if i, err := strconv.ParseUint(dd.V.(string), 0, 32); err == nil {
										rec.SourceAS = int(i)
									}
								case IPFIXFieldDestAS:
									if i, err := strconv.ParseUint(dd.V.(string), 0, 32); err == nil {
										rec.DestAS = int(i)
									}
								case IPFIXFIeldFlowEndTimestamp:
									rec.EndTime = int(dd.V.(float64))
								case IPFIXFIeldFlowStartTimestamp:
									rec.StartTime = int(dd.V.(float64))
								}
							}
							rec.FlowID = buildFlowID(rec)
							recChan <- rec
						}
					}
					c.consumer.MarkOffset(msg, "")
				}
			}
		}
	}()
	return recChan
}
