package queue

// Wire opcodes for Queue domain (low byte values).
const (
	QueueEnqueue  uint8 = 500 % 256
	QueueReserve  uint8 = 501 % 256
	QueueComplete uint8 = 502 % 256
)
