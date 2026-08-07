package fitz

import (
	"context"
	"sync"

	coreiter "github.com/cntryl/fitz-go/v2/internal/core/iter"
)

type managedPollResult[T any] struct {
	value     T
	emit      bool
	pollAgain bool
}

func startManagedPollingIterator[T any](
	ctx context.Context,
	subscribeWake func(context.Context, func()) (func(), error),
	poll func(context.Context) (managedPollResult[T], error),
) (Iterator[T], error) {
	helperCtx, cancel := context.WithCancel(ctx)
	gate := NewWakeGate()
	unsubscribe, err := subscribeWake(helperCtx, func() { gate.Wake() })
	if err != nil {
		cancel()
		return nil, err
	}

	values := make(chan T)
	errors := make(chan error, 1)
	go func() {
		defer close(values)
		defer close(errors)
		defer unsubscribe()

		var version uint64
		for {
			result, pollErr := poll(helperCtx)
			if pollErr != nil {
				errors <- pollErr
				return
			}
			if result.emit {
				select {
				case values <- result.value:
				case <-helperCtx.Done():
					errors <- context.Cause(helperCtx)
					return
				}
			}
			if result.pollAgain {
				continue
			}
			version, pollErr = gate.WaitAfter(helperCtx, version)
			if pollErr != nil {
				errors <- pollErr
				return
			}
		}
	}()

	return coreiter.NewChannelIterator[T](values, errors, cancel), nil
}

func startManagedPushIterator[T any](
	ctx context.Context,
	capacity int,
	subscribe func(context.Context, func(T) error) (func(), error),
) (Iterator[T], error) {
	helperCtx, cancel := context.WithCancel(ctx)
	values := make(chan T, capacity)
	errors := make(chan error, 1)
	var deliveryMu sync.RWMutex
	closed := false
	unsubscribe, err := subscribe(helperCtx, func(value T) error {
		deliveryMu.RLock()
		defer deliveryMu.RUnlock()
		if closed {
			return context.Cause(helperCtx)
		}
		select {
		case values <- value:
			return nil
		case <-helperCtx.Done():
			return context.Cause(helperCtx)
		}
	})
	if err != nil {
		cancel()
		return nil, err
	}
	go func() {
		<-helperCtx.Done()
		unsubscribe()
		deliveryMu.Lock()
		closed = true
		close(values)
		errors <- context.Cause(helperCtx)
		close(errors)
		deliveryMu.Unlock()
	}()
	return coreiter.NewChannelIterator[T](values, errors, cancel), nil
}
