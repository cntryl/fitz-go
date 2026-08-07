package connection

import (
	"encoding/binary"
	"errors"
	"testing"

	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func appendU32(dst []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(dst, encoded[:]...)
}

func TestShouldRejectEmptyPayloadGivenNoResponseBytesWhenParseStandardResponseCalled(t *testing.T) {
	success, remaining, err := ParseStandardResponse(nil)

	require.Error(t, err)
	assert.False(t, success)
	assert.Nil(t, remaining)
}

func TestShouldReturnRemainingPayloadGivenSuccessStatusWhenParseStandardResponseCalled(t *testing.T) {
	payload := []byte{0, 0x12, 0x34}

	success, remaining, err := ParseStandardResponse(payload)

	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, []byte{0x12, 0x34}, remaining)
}

func TestShouldParseTypedDomainErrorGivenKnownCodeWhenParseStandardResponseCalled(t *testing.T) {
	payload := []byte{1}
	payload = appendU32(payload, uint32(coreerrors.KvIsolationConflict))
	payload = appendU32(payload, 4)
	payload = append(payload, []byte("boom")...)

	success, remaining, err := ParseStandardResponse(payload)

	assert.False(t, success)
	assert.Nil(t, remaining)
	require.Error(t, err)

	var domainErr *coreerrors.DomainError
	require.True(t, errors.As(err, &domainErr))
	assert.Equal(t, coreerrors.ErrorCode(coreerrors.KvIsolationConflict), domainErr.Code)
	assert.Equal(t, "boom", domainErr.Message)
}

func TestShouldRejectStringOnlyErrorGivenMissingDomainCodeWhenParseStandardResponseCalled(t *testing.T) {
	payload := []byte{1}
	payload = appendU32(payload, 5)
	payload = append(payload, []byte("hello")...)

	success, remaining, err := ParseStandardResponse(payload)

	assert.False(t, success)
	assert.Nil(t, remaining)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode domain error")

	var domainErr *coreerrors.DomainError
	assert.False(t, errors.As(err, &domainErr))
}

func TestShouldReturnDecodeErrorGivenTruncatedTypedDomainErrorWhenParseStandardResponseCalled(t *testing.T) {
	payload := []byte{1}
	payload = appendU32(payload, uint32(coreerrors.KvIsolationConflict))
	payload = appendU32(payload, 10)
	payload = append(payload, []byte("abc")...)

	success, remaining, err := ParseStandardResponse(payload)

	assert.False(t, success)
	assert.Nil(t, remaining)
	require.Error(t, err)

	var domainErr *coreerrors.DomainError
	assert.False(t, errors.As(err, &domainErr))
}

func TestShouldParseStringOnlyDomainErrorWhenParsePlainResponseCalled(t *testing.T) {
	payload := []byte{1}
	payload = appendU32(payload, 5)
	payload = append(payload, []byte("hello")...)

	success, remaining, err := ParsePlainResponse(payload)

	assert.False(t, success)
	assert.Nil(t, remaining)
	require.EqualError(t, err, "hello")
}
