package kv

import (
	"testing"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
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

	t.Run("preserve unknown typed error", func(t *testing.T) {
		errMsg := coreerrors.NewDomainError(coreerrors.KvRealmMismatch, "realm mismatch")
		mapped := mapKVError(errMsg)

		var domainErr *coreerrors.DomainError
		assert.ErrorAs(t, mapped, &domainErr)
		assert.Equal(t, uint32(coreerrors.KvRealmMismatch), uint32(domainErr.Code))
	})
}

func TestShouldRejectInvertedRangeGivenScanQueryWhenEncodeScanCalled(t *testing.T) {
	_, err := EncodeScan(1, "kv://realm/area/resource", ScanQuery{
		StartKey: []byte("z"),
		EndKey:   []byte("a"),
		Limit:    10,
	})
	assert.ErrorIs(t, err, ErrInvalidRange)
}
