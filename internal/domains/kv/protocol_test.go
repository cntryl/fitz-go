package kv

import (
	"errors"
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
		assert.True(t, errors.As(mapped, &domainErr))
		assert.Equal(t, uint32(coreerrors.KvRealmMismatch), uint32(domainErr.Code))
	})
}
