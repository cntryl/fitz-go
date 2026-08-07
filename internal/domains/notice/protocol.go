package notice

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/cntryl/fitz-go/v2/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
)

// Wire operation codes for Notice domain. Values are message type identifiers.
const (
	NoticePublish        uint16 = 500
	NoticeSubscribe      uint16 = 501
	NoticeUnsubscribe    uint16 = 502
	NoticeUnsubscribeAll uint16 = 503
	NoticeNotify         uint16 = 504
)

// Domain-specific errors. Returned when the server rejects a request or the operation fails.
//   - ErrNoticeRouteInvalid: publish or subscribe route/pattern is invalid.
//   - ErrNoticeTimeout: the operation timed out.
//   - ErrNoticeSendFailed: publish or subscribe request failed.
var (
	ErrNoticeRouteInvalid = errors.New("invalid notice route")
	ErrNoticeTimeout      = errors.New("notice operation timed out")
	ErrNoticeSendFailed   = errors.New("notice send failed")
)

// mapNoticeError maps a broker error to a domain-specific Go error.
func mapNoticeError(err error) error {
	if err == nil {
		return nil
	}

	var domainErr *coreerrors.DomainError
	if errors.As(err, &domainErr) {
		switch uint32(domainErr.Code) {
		case coreerrors.NoticeInvalidRoute, coreerrors.NoticeInvalidPattern:
			return ErrNoticeRouteInvalid
		default:
			return err
		}
	}

	l := strings.ToLower(err.Error())
	switch {
	case strings.Contains(l, "route") && (strings.Contains(l, "invalid") || strings.Contains(l, "bad")):
		return ErrNoticeRouteInvalid
	case strings.Contains(l, "timeout") || strings.Contains(l, "timed out"):
		return ErrNoticeTimeout
	case strings.Contains(l, "send") || strings.Contains(l, "failed"):
		return ErrNoticeSendFailed
	default:
		return err
	}
}

// ---------------------------------------------------------------------------
// Wire encoding / decoding helpers (custom binary format, not TLV)
// ---------------------------------------------------------------------------

func encodePublish(route string, body []byte) []byte {
	routeBytes := []byte(route)
	totalLen := 4 + len(routeBytes) + 4 + len(body)
	buf := make([]byte, 0, totalLen)
	buf = appendU32(buf, uint32(len(routeBytes)))
	buf = append(buf, routeBytes...)
	buf = appendU32(buf, uint32(len(body)))
	buf = append(buf, body...)
	return buf
}

func publishPayloadWriter(route string, body []byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, body)
	}
}

func encodeSubscribe(route string) []byte {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	})
}

func subscribePayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

func encodeUnsubscribe(subID uint64) []byte {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, subID)
	})
}

func unsubscribePayloadWriter(subID uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, subID)
	}
}

func DecodeNotify(body []byte) (string, []byte, bool) {
	_, route, ok := decodeFirstRoute(body)
	if !ok {
		return "", nil, false
	}
	payload, ok := decodePayload(body)
	if !ok {
		return "", nil, false
	}
	return route, payload, true
}

func decodeFirstRoute(body []byte) (int, string, bool) {
	if len(body) < 4 {
		return 0, "", false
	}
	idx := 0
	routeLen := readU32(body[idx:])
	idx += 4
	if idx+int(routeLen) > len(body) {
		return 0, "", false
	}
	route := string(body[idx : idx+int(routeLen)])
	idx += int(routeLen)
	return idx, route, true
}

func decodePayload(body []byte) ([]byte, bool) {
	idx, _, ok := decodeFirstRoute(body)
	if !ok || idx+4 > len(body) {
		return nil, false
	}
	plen := readU32(body[idx:])
	idx += 4
	if idx+int(plen) > len(body) {
		return nil, false
	}
	if idx+int(plen) != len(body) {
		return nil, false
	}
	payload := append([]byte(nil), body[idx:idx+int(plen)]...)
	return payload, true
}

func decodeStatus(body []byte) (uint8, uint32, string, bool) {
	if len(body) < 1 {
		return 0, 0, "", false
	}
	status := body[0]
	if status == 0 {
		if len(body) != 1 {
			return 0, 0, "", false
		}
		return 0, 0, "", true
	}
	if status != 1 || len(body) < 9 {
		return 0, 0, "", false
	}
	code := readU32(body[1:5])
	msgLen := readU32(body[5:9])
	if uint64(9)+uint64(msgLen) != uint64(len(body)) {
		return 0, 0, "", false
	}
	return status, code, string(body[9 : 9+msgLen]), true
}

func IsNoticeResponseType(t uint16) bool {
	return t == NoticePublish || t == NoticeSubscribe || t == NoticeUnsubscribe || t == NoticeUnsubscribeAll
}

func DecodeNoticeResponseKey(op uint16, body []byte) (string, error) {
	status, code, errMsg, ok := decodeStatus(body)
	if !ok {
		return "", nil
	}
	if status != 0 {
		return "", coreerrors.NewDomainError(code, errMsg)
	}
	return NoticeWaitKey(op), nil
}

func NoticeWaitKey(op uint16) string {
	return strconv.FormatUint(uint64(op), 10)
}

func appendU32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func readU32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// ---------------------------------------------------------------------------
// Route validation
// ---------------------------------------------------------------------------

func stripNoticeScheme(route string) string {
	const prefix = "notice://"
	if len(route) >= len(prefix) && route[:len(prefix)] == prefix {
		return route[len(prefix):]
	}
	return route
}

func ValidateNoticeRoute(route string, allowWildcards bool) error {
	if len(route) < 9 || route[:9] != "notice://" {
		return errors.New("notice route must start with notice://")
	}
	path := stripNoticeScheme(route)
	segs := splitSlash(path)
	if len(segs) != 3 {
		if allowWildcards && len(segs) == 2 && segs[1] == "**" {
			return nil
		}
		return errors.New("notice route must have realm/area/resource")
	}
	if segs[0] == "" || segs[1] == "" || segs[2] == "" {
		return errors.New("notice route segments must be non-empty")
	}
	if !allowWildcards {
		if containsWildcard(segs[1]) || containsWildcard(segs[2]) {
			return errors.New("notice publish route cannot contain wildcards")
		}
		return nil
	}
	if segs[0] == "*" || segs[0] == "**" {
		return errors.New("notice realm cannot be wildcard")
	}
	return nil
}

// NoticeMatchRoute matches a subscription pattern against a notification route.
func NoticeMatchRoute(pattern, route string) bool {
	pat := stripNoticeScheme(pattern)
	rt := stripNoticeScheme(route)
	if pat == rt {
		return true
	}
	pSegs := splitSlash(pat)
	rSegs := splitSlash(rt)
	pi, ri := 0, 0
	for pi < len(pSegs) && ri < len(rSegs) {
		if pSegs[pi] == "**" {
			if pi == len(pSegs)-1 {
				return true
			}
			next := pSegs[pi+1]
			for ri < len(rSegs) {
				if rSegs[ri] == next {
					break
				}
				ri++
			}
			pi++
			continue
		}
		if pSegs[pi] == "*" {
			pi++
			ri++
			continue
		}
		if pSegs[pi] != rSegs[ri] {
			return false
		}
		pi++
		ri++
	}
	for pi < len(pSegs) && pSegs[pi] == "**" {
		pi++
	}
	return pi == len(pSegs) && ri == len(rSegs)
}

// ---------------------------------------------------------------------------
// Tiny helpers (avoid importing strings for two uses)
// ---------------------------------------------------------------------------

func splitSlash(s string) []string {
	var parts []string
	start := 0
	for i := range len(s) {
		if s[i] == '/' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func containsWildcard(s string) bool {
	for i := range len(s) {
		if s[i] == '*' {
			return true
		}
	}
	return false
}
