package kv

import (
	"bytes"
	"testing"

	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/stretchr/testify/assert"
)

func TestShouldMapKVErrorGivenTypedBrokerMessageWhenMapKVErrorCalled(t *testing.T) {
	t.Run("map key not found", func(t *testing.T) {
		mapped := mapKVError(coreerrors.NewDomainError(coreerrors.KvKeyNotFound, "key not found"))
		assert.Equal(t, ErrNotFound, mapped)
	})

	t.Run("map isolation conflict", func(t *testing.T) {
		mapped := mapKVError(coreerrors.NewDomainError(coreerrors.KvIsolationConflict, "isolation conflict"))
		assert.Equal(t, ErrConcurrencyConflict, mapped)
	})

	t.Run("map readonly write", func(t *testing.T) {
		mapped := mapKVError(coreerrors.NewDomainError(coreerrors.KvWriteInReadonly, "write in readonly"))
		assert.Equal(t, ErrReadOnlyTransaction, mapped)
	})

	t.Run("map legacy backend-coded readonly write by canonical message", func(t *testing.T) {
		mapped := mapKVError(coreerrors.NewDomainError(coreerrors.KvBackendError, "Backend error: Cannot write in ReadOnly transaction"))
		assert.Equal(t, ErrReadOnlyTransaction, mapped)
	})

	t.Run("preserve unknown typed error", func(t *testing.T) {
		errMsg := coreerrors.NewDomainError(coreerrors.KvRealmMismatch, "realm mismatch")
		mapped := mapKVError(errMsg)

		var domainErr *coreerrors.DomainError
		assert.ErrorAs(t, mapped, &domainErr)
		assert.Equal(t, uint32(coreerrors.KvRealmMismatch), uint32(domainErr.Code))
	})
}

func TestShouldEncodeInvertedRangeGivenScanQueryWhenEncodeScanCalled(t *testing.T) {
	payload, err := encodeScan(1, "kv://realm/area/resource", ScanQuery{
		StartKey: []byte("z"),
		EndKey:   []byte("a"),
		Limit:    10,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, payload)
}

func TestShouldEncodeReverseRangeGivenDescendingBoundsWhenEncodeScanCalled(t *testing.T) {
	payload, err := encodeScan(1, "kv://realm/area/resource", ScanQuery{
		StartKey: []byte("z"),
		EndKey:   []byte("a"),
		Reverse:  true,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, payload)
}

func TestShouldEncodeExclusiveFlagOnlyWhenRequestedGivenScanQuery(t *testing.T) {
	query := ScanQuery{StartKey: []byte("key"), StartExclusive: true}
	payload, err := encodeScan(1, "kv://realm/area/resource", query)
	assert.NoError(t, err)
	assert.Equal(t, byte(1), payload[len(payload)-1])

	writer, err := scanPayloadWriter(1, "kv://realm/area/resource", query)
	assert.NoError(t, err)
	var encoded bytes.Buffer
	writer(&encoded)
	assert.Equal(t, payload, encoded.Bytes())

	query.StartExclusive = false
	legacyPayload, err := encodeScan(1, "kv://realm/area/resource", query)
	assert.NoError(t, err)
	assert.NotEqual(t, len(payload), len(legacyPayload))
}
