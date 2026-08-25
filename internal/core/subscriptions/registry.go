package subscriptions

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
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

	restoreEntriesScratch  []*entry[H]
	restoreRollbackScratch []restoreEntry
	restoreSubIDsScratch   []uint64
	restoreBySubIDScratch  map[uint64]*entry[H]
}

// Completion reports the terminal state of a local subscription handler.
// The channel yields exactly one result: nil after an explicit unsubscribe,
// or the terminal error that caused the handler to be removed.
type Completion struct {
	once sync.Once
	done chan error
}

func NewCompletion() *Completion {
	return &Completion{done: make(chan error, 1)}
}

func (c *Completion) Done() <-chan error {
	if c == nil {
		return nil
	}
	return c.done
}

func (c *Completion) Complete(err error) {
	if c == nil {
		return
	}
	c.once.Do(func() {
		c.done <- err
		close(c.done)
	})
}

type Registration[H any] struct {
	Pattern    string
	HandlerID  uint64
	Handler    H
	Completion *Completion
}

type pendingSubscribe struct {
	done chan struct{}
}

type entry[H any] struct {
	pattern  string
	subID    uint64
	handlers map[uint64]Registration[H]
}

type restoreEntry struct {
	pattern string
	subID   uint64
}

func NewRegistry[H any]() *Registry[H] {
	r := &Registry[H]{
		byPattern:             make(map[string]*entry[H]),
		bySubID:               make(map[uint64]*entry[H]),
		pending:               make(map[string]*pendingSubscribe),
		restoreBySubIDScratch: make(map[uint64]*entry[H]),
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
			existing.handlers[handlerID] = Registration[H]{
				Pattern: pattern, HandlerID: handlerID, Handler: handler, Completion: NewCompletion(),
			}
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
			handlers: map[uint64]Registration[H]{
				handlerID: {
					Pattern: pattern, HandlerID: handlerID, Handler: handler, Completion: NewCompletion(),
				},
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

	handler, exists := registered.handlers[handlerID]
	if !exists {
		return false
	}
	handler.Completion.Complete(nil)
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
	for _, registration := range registered.handlers {
		handlers = append(handlers, registration.Handler)
	}
	return handlers
}

func (r *Registry[H]) Registrations(subID uint64) []Registration[H] {
	r.mu.Lock()
	defer r.mu.Unlock()

	registered, ok := r.bySubID[subID]
	if !ok {
		return nil
	}

	registrations := make([]Registration[H], 0, len(registered.handlers))
	for _, registration := range registered.handlers {
		registrations = append(registrations, registration)
	}
	return registrations
}

func (r *Registry[H]) Completion(pattern string, handlerID uint64) *Completion {
	r.mu.Lock()
	defer r.mu.Unlock()
	registered, ok := r.byPattern[pattern]
	if !ok {
		return nil
	}
	registration, ok := registered.handlers[handlerID]
	if !ok {
		return nil
	}
	return registration.Completion
}

func (r *Registry[H]) Restore(wireSubscribe func(string) (uint64, error), wireUnsubscribe func(string, uint64) error) error {
	r.mu.Lock()
	locked := true
	defer func() {
		if locked {
			r.mu.Unlock()
		}
		r.finishRestore()
	}()
	for r.restoring || len(r.pending) > 0 {
		r.cond.Wait()
	}

	if len(r.byPattern) == 0 {
		return nil
	}
	r.restoring = true

	entries := r.restoreEntriesScratch[:0]
	if cap(entries) < len(r.byPattern) {
		entries = make([]*entry[H], 0, len(r.byPattern))
	}
	for _, registered := range r.byPattern {
		entries = append(entries, registered)
	}
	slices.SortFunc(entries, func(left, right *entry[H]) int {
		return cmp.Compare(left.pattern, right.pattern)
	})

	restoredEntries := r.restoreRollbackScratch[:0]
	if cap(restoredEntries) < len(entries) {
		restoredEntries = make([]restoreEntry, 0, len(entries))
	}
	if cap(r.restoreSubIDsScratch) < len(entries) {
		r.restoreSubIDsScratch = make([]uint64, len(entries))
	}
	restoredSubIDs := r.restoreSubIDsScratch[:len(entries)]
	restoredBySubID := r.restoreBySubIDScratch
	if restoredBySubID == nil {
		restoredBySubID = make(map[uint64]*entry[H], len(entries))
	}
	for subID := range restoredBySubID {
		delete(restoredBySubID, subID)
	}
	r.mu.Unlock()
	locked = false

	for idx, registered := range entries {
		subID, err := wireSubscribe(registered.pattern)
		if err != nil {
			if rollbackErr := r.rollbackRestore(restoredEntries, wireUnsubscribe); rollbackErr != nil {
				return errors.Join(err, rollbackErr)
			}
			return err
		}
		if _, exists := restoredBySubID[subID]; exists {
			if rollbackErr := r.rollbackRestore(append(restoredEntries, restoreEntry{pattern: registered.pattern, subID: subID}), wireUnsubscribe); rollbackErr != nil {
				return errors.Join(fmt.Errorf("duplicate subscription_id %d while restoring pattern %q", subID, registered.pattern), rollbackErr)
			}
			return fmt.Errorf("duplicate subscription_id %d while restoring pattern %q", subID, registered.pattern)
		}

		restoredEntries = append(restoredEntries, restoreEntry{pattern: registered.pattern, subID: subID})
		restoredSubIDs[idx] = subID
		restoredBySubID[subID] = registered
	}

	r.mu.Lock()
	locked = true
	for idx, registered := range entries {
		registered.subID = restoredSubIDs[idx]
	}
	oldBySubID := r.bySubID
	r.bySubID = restoredBySubID
	r.restoreEntriesScratch = entries[:0]
	r.restoreRollbackScratch = restoredEntries[:0]
	r.restoreSubIDsScratch = restoredSubIDs[:0]
	r.restoreBySubIDScratch = oldBySubID
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
	for _, v := range slices.Backward(entries) {
		entry := v
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
