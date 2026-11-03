package connection

import "github.com/cntryl/fitz-go/internal/protocol"

// SupportsCapability reports a numeric capability negotiated for this live session.
func (c *Connection) SupportsCapability(id uint32) bool {
	return id >= 1 && id <= 2 && c.mux.protocolVersion.Load() == 3 && c.mux.activeCapabilityIDs.Load()&(1<<id) != 0
}

func (c *Connection) CapabilityIDs() []uint32 {
	var result []uint32
	for _, id := range []uint32{protocol.CapabilityQueueMessageTTL, protocol.CapabilityQueueNack} {
		if c.SupportsCapability(id) {
			result = append(result, id)
		}
	}
	return result
}
