package notice

// Channel and wire codes for Notice domain (per CLIENT_SPEC.md semantics).
// Channels: Pub (publish) and Sub (subscribe) are reserved 1/2 in the protocol.
const (
	ChannelPub uint32 = 1
	ChannelSub uint32 = 2
)

// Wire operation codes for Notice domain. Values are low-byte uint8 equivalents.
const (
	NoticeSubscribe      uint8 = 200 % 256
	NoticeUnsubscribe    uint8 = 201 % 256
	NoticePublish        uint8 = 202 % 256
	NoticeUnsubscribeAll uint8 = 203 % 256
)
