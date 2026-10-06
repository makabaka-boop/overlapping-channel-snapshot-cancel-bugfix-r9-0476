package main

import (
	"context"
	"sync"
)

// Mailbox is an unbounded, context-aware FIFO queue. Each node event loop and
// coordinator owns one so producers never block on a full channel.
type Mailbox[T any] struct {
	mu      sync.Mutex
	items   []T
	waiters []chan T
	dead    bool
}

func NewMailbox[T any](lifetime context.Context) *Mailbox[T] {
	m := &Mailbox[T]{}

	go func() {
		<-lifetime.Done()
		m.mu.Lock()
		m.dead = true
		waiters := m.waiters
		m.waiters = nil
		m.mu.Unlock()

		for _, waiter := range waiters {
			close(waiter)
		}
	}()

	return m
}

func (m *Mailbox[T]) Put(item T) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		return false
	}

	// Wake the oldest waiter first, preserving FIFO fairness between event
	// loops. Otherwise append to the durable queue.
	if len(m.waiters) > 0 {
		waiter := m.waiters[0]
		m.waiters = m.waiters[1:]
		waiter <- item
		return true
	}

	m.items = append(m.items, item)
	return true
}

func (m *Mailbox[T]) Recv(ctx context.Context) (T, bool) {
	var zero T

	m.mu.Lock()
	if len(m.items) > 0 {
		item := m.items[0]
		m.items = m.items[1:]
		m.mu.Unlock()
		return item, true
	}
	if m.dead {
		m.mu.Unlock()
		return zero, false
	}

	waiter := make(chan T, 1)
	m.waiters = append(m.waiters, waiter)
	m.mu.Unlock()

	select {
	case item, ok := <-waiter:
		return item, ok
	case <-ctx.Done():
		m.mu.Lock()
		for i, candidate := range m.waiters {
			if candidate == waiter {
				m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
				break
			}
		}
		dead := m.dead
		m.mu.Unlock()

		// A concurrent Put may have selected this waiter immediately before
		// cancellation removed it.
		select {
		case item, ok := <-waiter:
			if ok {
				_ = m.Put(item)
			}
			return item, ok
		default:
		}
		if dead {
			return zero, false
		}
		return zero, false
	}
}

func (m *Mailbox[T]) Depth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.items)
}
