package consumer

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net"
	"time"
)

type FlowRecord struct {
	FlowID             FlowID
	AgentID            string
	ExportTime         time.Time
	OctetDelta         uint64
	PacketDelta        uint64
	DeltaFlowCount     uint64
	Protocol           int
	IPClass            int
	SourcePort         int
	SourceAddress      net.IP
	SourcePrefixLength int
	IngressInterface   int
	DestPort           int
	DestAddress        net.IP
	DestPrefixLength   int
	EgressInterface    int
	NextHopAddress     net.IP
	SourceAS           int
	DestAS             int

	StartTime int
	EndTime   int
}

type FlowID string

func buildFlowID(flow *FlowRecord) FlowID {
	h := fnv.New128()
	h.Write([]byte(flow.AgentID))
	h.Write([]byte(flow.SourceAddress))
	h.Write([]byte(fmt.Sprintf("%d", flow.SourcePort)))
	h.Write([]byte(flow.DestAddress))
	h.Write([]byte(fmt.Sprintf("%d", flow.DestPort)))
	h.Sum([]byte{})
	return FlowID(hex.Dump(h.Sum([]byte{})))
}
