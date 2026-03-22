package subscriptions

import (
	"fmt"
	"sort"
	"sync"
)

type Registry[H any] struct {
	mu            sync.Mutex
	byPattern     map[string]*entry[H]
	bySubID       map[uint64]*entry[H]
	nextHandlerID uint64
}

type entry[H any] struct {
	pattern  string
	subID    uint64
	handlers map[uint64]H
}

func NewRegistry[H any]() *Registry[H] {
	return &Registry[H]{
		byPattern: make(map[string]*entry[H]),
		bySubID:   make(map[uint64]*entry[H]),
	}
}

func (r *Registry[H]) Subscribe(pattern string, handler H, wireSubscribe func(string) (uint64, error)) (uint64, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.byPattern[pattern]; ok {
		handlerID := r.nextHandler()
		existing.handlers[handlerID] = handler
		return existing.subID, handlerID, nil
	}

	subID, err := wireSubscribe(pattern)
	if err != nil {
		return 0, 0, err
	}
	if _, exists := r.bySubID[subID]; exists {
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
	return subID, handlerID, nil
}

func (r *Registry[H]) Unsubscribe(pattern string, handlerID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

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

func (r *Registry[H]) Restore(wireSubscribe func(string) (uint64, error)) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.byPattern) == 0 {
		return nil
	}

	patterns := make([]string, 0, len(r.byPattern))
	for pattern := range r.byPattern {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns)

	restoredByPattern := make(map[string]*entry[H], len(patterns))
	restoredBySubID := make(map[uint64]*entry[H], len(patterns))

	for _, pattern := range patterns {
		registered := r.byPattern[pattern]
		subID, err := wireSubscribe(pattern)
		if err != nil {
			return err
		}
		if _, exists := restoredBySubID[subID]; exists {
			return fmt.Errorf("duplicate subscription_id %d while restoring pattern %q", subID, pattern)
		}

		handlers := make(map[uint64]H, len(registered.handlers))
		for handlerID, handler := range registered.handlers {
			handlers[handlerID] = handler
		}

		restored := &entry[H]{
			pattern:  pattern,
			subID:    subID,
			handlers: handlers,
		}
		restoredByPattern[pattern] = restored
		restoredBySubID[subID] = restored
	}

	r.byPattern = restoredByPattern
	r.bySubID = restoredBySubID
	return nil
}

func (r *Registry[H]) nextHandler() uint64 {
	r.nextHandlerID++
	return r.nextHandlerID
}
