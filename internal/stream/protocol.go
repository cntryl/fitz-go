package stream

// Channel used for Stream domain traffic.
const (
	ChannelStream uint32 = 7
)

// Wire opcodes for Stream domain (low byte values).
const (
	StreamBegin    uint8 = 300 % 256
	StreamAppend   uint8 = 301 % 256
	StreamRead     uint8 = 302 % 256
	StreamRollback uint8 = 303 % 256
	StreamLast     uint8 = 304 % 256
)
