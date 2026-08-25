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
	subscribeWake func(context.Context, func()) (func(), <-chan error, error),
	poll func(context.Context) (managedPollResult[T], error),
) (Iterator[T], error) {
	helperCtx, cancel := context.WithCancelCause(ctx)
	gate := NewWakeGate()
	unsubscribe, completion, err := subscribeWake(helperCtx, func() { gate.Wake() })
	if err != nil {
		cancel(err)
		return nil, err
	}
	monitorSubscriptionCompletion(helperCtx, cancel, completion)

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

	return coreiter.NewChannelIterator[T](values, errors, func() { cancel(context.Canceled) }), nil
}

func startManagedPushIterator[T any](
	ctx context.Context,
	capacity int,
	subscribe func(context.Context, func(T) error) (func(), <-chan error, error),
) (Iterator[T], error) {
	helperCtx, cancel := context.WithCancelCause(ctx)
	values := make(chan T, capacity)
	errors := make(chan error, 1)
	var deliveryMu sync.RWMutex
	closed := false
	unsubscribe, completion, err := subscribe(helperCtx, func(value T) error {
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
		cancel(err)
		return nil, err
	}
	monitorSubscriptionCompletion(helperCtx, cancel, completion)
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
	return coreiter.NewChannelIterator[T](values, errors, func() { cancel(context.Canceled) }), nil
}

func monitorSubscriptionCompletion(ctx context.Context, cancel context.CancelCauseFunc, completion <-chan error) {
	if completion == nil {
		return
	}
	go func() {
		select {
		case err := <-completion:
			if err != nil {
				cancel(err)
			}
		case <-ctx.Done():
		}
	}()
}
