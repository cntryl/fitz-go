package subscriptions

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

type Registry[H any] struct {
	mu            sync.Mutex
	cond          *sync.Cond
	byPattern     map[string]*entry[H]
	bySubID       map[uint64]*entry[H]
	pending       map[string]*pendingSubscribe
	restoring     bool
	nextHandlerID uint64
}

type pendingSubscribe struct {
	done chan struct{}
}

type entry[H any] struct {
	pattern  string
	subID    uint64
	handlers map[uint64]H
}

type restoreEntry struct {
	pattern string
	subID   uint64
}

func NewRegistry[H any]() *Registry[H] {
	r := &Registry[H]{
		byPattern: make(map[string]*entry[H]),
		bySubID:   make(map[uint64]*entry[H]),
		pending:   make(map[string]*pendingSubscribe),
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *Registry[H]) Subscribe(pattern string, handler H, wireSubscribe func(string) (uint64, error)) (uint64, uint64, error) {
	for {
		r.mu.Lock()
		for r.restoring {
			r.cond.Wait()
		}

		if existing, ok := r.byPattern[pattern]; ok {
			handlerID := r.nextHandler()
			existing.handlers[handlerID] = handler
			r.mu.Unlock()
			return existing.subID, handlerID, nil
		}

		if pending, ok := r.pending[pattern]; ok {
			done := pending.done
			r.mu.Unlock()
			<-done
			continue
		}

		pending := &pendingSubscribe{done: make(chan struct{})}
		r.pending[pattern] = pending
		r.mu.Unlock()

		subID, err := wireSubscribe(pattern)

		r.mu.Lock()
		delete(r.pending, pattern)
		close(pending.done)
		r.cond.Broadcast()
		if err != nil {
			r.mu.Unlock()
			return 0, 0, err
		}
		if _, exists := r.bySubID[subID]; exists {
			r.mu.Unlock()
			return 0, 0, fmt.Errorf("duplicate subscription_id %d for pattern %q", subID, pattern)
		}

		handlerID := r.nextHandler()
		registered := &entry[H]{
			pattern: pattern,
			subID:   subID,
			handlers: map[uint64]H{
				handlerID: handler,
			},
		}
		r.byPattern[pattern] = registered
		r.bySubID[subID] = registered
		r.mu.Unlock()
		return subID, handlerID, nil
	}
}

func (r *Registry[H]) Unsubscribe(pattern string, handlerID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.restoring {
		r.cond.Wait()
	}

	registered, ok := r.byPattern[pattern]
	if !ok {
		return false
	}

	delete(registered.handlers, handlerID)
	if len(registered.handlers) != 0 {
		return false
	}

	delete(r.byPattern, pattern)
	delete(r.bySubID, registered.subID)
	return true
}

func (r *Registry[H]) Handlers(subID uint64) []H {
	r.mu.Lock()
	defer r.mu.Unlock()

	registered, ok := r.bySubID[subID]
	if !ok {
		return nil
	}

	handlers := make([]H, 0, len(registered.handlers))
	for _, handler := range registered.handlers {
		handlers = append(handlers, handler)
	}
	return handlers
}

func (r *Registry[H]) Restore(wireSubscribe func(string) (uint64, error), wireUnsubscribe func(string, uint64) error) error {
	r.mu.Lock()
	for r.restoring || len(r.pending) > 0 {
		r.cond.Wait()
	}

	if len(r.byPattern) == 0 {
		r.mu.Unlock()
		return nil
	}
	r.restoring = true
	defer r.finishRestore()

	patterns := make([]string, 0, len(r.byPattern))
	handlersByPattern := make(map[string]map[uint64]H, len(r.byPattern))
	for pattern := range r.byPattern {
		patterns = append(patterns, pattern)
		registered := r.byPattern[pattern]
		handlers := make(map[uint64]H, len(registered.handlers))
		for handlerID, handler := range registered.handlers {
			handlers[handlerID] = handler
		}
		handlersByPattern[pattern] = handlers
	}
	sort.Strings(patterns)
	r.mu.Unlock()

	restoredByPattern := make(map[string]*entry[H], len(patterns))
	restoredBySubID := make(map[uint64]*entry[H], len(patterns))
	restoredEntries := make([]restoreEntry, 0, len(patterns))

	for _, pattern := range patterns {
		subID, err := wireSubscribe(pattern)
		if err != nil {
			if rollbackErr := r.rollbackRestore(restoredEntries, wireUnsubscribe); rollbackErr != nil {
				return errors.Join(err, rollbackErr)
			}
			return err
		}
		if _, exists := restoredBySubID[subID]; exists {
			if rollbackErr := r.rollbackRestore(append(restoredEntries, restoreEntry{pattern: pattern, subID: subID}), wireUnsubscribe); rollbackErr != nil {
				return errors.Join(fmt.Errorf("duplicate subscription_id %d while restoring pattern %q", subID, pattern), rollbackErr)
			}
			return fmt.Errorf("duplicate subscription_id %d while restoring pattern %q", subID, pattern)
		}

		restoredEntries = append(restoredEntries, restoreEntry{pattern: pattern, subID: subID})
		restored := &entry[H]{
			pattern:  pattern,
			subID:    subID,
			handlers: handlersByPattern[pattern],
		}
		restoredByPattern[pattern] = restored
		restoredBySubID[subID] = restored
	}

	r.mu.Lock()
	r.byPattern = restoredByPattern
	r.bySubID = restoredBySubID
	r.cond.Broadcast()
	r.mu.Unlock()
	return nil
}

func (r *Registry[H]) finishRestore() {
	r.mu.Lock()
	if r.restoring {
		r.restoring = false
		r.cond.Broadcast()
	}
	r.mu.Unlock()
}

func (r *Registry[H]) rollbackRestore(entries []restoreEntry, wireUnsubscribe func(string, uint64) error) error {
	if wireUnsubscribe == nil || len(entries) == 0 {
		return nil
	}

	var rollbackErr error
	for idx := len(entries) - 1; idx >= 0; idx-- {
		entry := entries[idx]
		if err := wireUnsubscribe(entry.pattern, entry.subID); err != nil {
			rollbackErr = errors.Join(rollbackErr, err)
		}
	}
	return rollbackErr
}

func (r *Registry[H]) nextHandler() uint64 {
	r.nextHandlerID++
	return r.nextHandlerID
}
