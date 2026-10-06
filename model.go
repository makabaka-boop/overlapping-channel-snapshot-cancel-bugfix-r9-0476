package main

import (
	"context"
	"sync"
	"sync/atomic"
)

const (
	nodeCount      = 3
	initialBalance = 100
)

type transfer struct {
	ID     string `json:"id"`
	From   int    `json:"from"`
	To     int    `json:"to"`
	Amount int    `json:"amount"`
}

type marker struct {
	SnapshotID int64
	From       int
}

type envelope struct {
	transfer *transfer
	marker   *marker
}

// link is one directed reliable FIFO channel. The test barrier only withholds
// delivery from that one queue; it never freezes a sender or global queue.
type link struct {
	from, to int
	outbox   *Mailbox[*envelope]

	mu       sync.Mutex
	held     bool
	inFlight int
	cond     *sync.Cond
}

func newLink(ctx context.Context, from, to int) *link {
	l := &link{
		from:   from,
		to:     to,
		outbox: NewMailbox[*envelope](ctx),
	}
	l.cond = sync.NewCond(&l.mu)

	go func() {
		<-ctx.Done()
		l.mu.Lock()
		l.cond.Broadcast()
		l.mu.Unlock()
	}()

	return l
}

// enqueue is safe to call from a sender's atomic state transition. It never
// waits for the test barrier.
func (l *link) enqueue(item *envelope) {
	l.outbox.Put(item)
}

func (l *link) dispatch(ctx context.Context, destination *Mailbox[nodeEvent]) {
	for {
		item, ok := l.outbox.Recv(ctx)
		if !ok {
			return
		}

		l.mu.Lock()
		l.inFlight++
		for l.held && ctx.Err() == nil {
			l.cond.Wait()
		}
		l.inFlight--
		l.mu.Unlock()

		if ctx.Err() != nil {
			return
		}

		// One dispatcher per directed link preserves per-channel FIFO across
		// the barrier and into the destination node's private event loop.
		if !destination.Put(nodeEvent{ingress: item}) {
			return
		}
	}
}

func (l *link) setHeld(held bool) {
	l.mu.Lock()
	l.held = held
	if !held {
		l.cond.Broadcast()
	}
	l.mu.Unlock()
}

func (l *link) snapshot() linkStatusView {
	l.mu.Lock()
	held := l.held
	inFlight := l.inFlight
	l.mu.Unlock()

	return linkStatusView{
		From:        l.from + 1,
		To:          l.to + 1,
		Held:        held,
		QueuedItems: l.outbox.Depth() + inFlight,
	}
}

type nodeEvent struct {
	cmd     nodeCommand
	ingress *envelope
}

type nodeCommand struct {
	transferCmd *transferCmd
	retire      *beginSnapshot
	begin       *beginSnapshot
}

type transferCmd struct {
	t     transfer
	reply chan error
}

type beginSnapshot struct {
	id    int64
	reply chan error
}

type snapshotChannelState struct {
	amount    int
	transfers []transfer
	closed    bool
}

type nodeSnapshotSession struct {
	id        int64
	channels  map[int]*snapshotChannelState
	remaining int
}

type reportKind int

const (
	reportLocal reportKind = iota
	reportChannel
)

type snapshotReport struct {
	nodeID int
	kind   reportKind

	snapshotID int64
	balance    int
	from       int
	state      *snapshotChannelState
}

type node struct {
	id      int
	balance int

	events  *Mailbox[nodeEvent]
	links   map[int]*link
	active  map[int64]*nodeSnapshotSession
	retired map[int64]bool

	report func(snapshotReport)
}

func newNode(ctx context.Context, id int, links map[int]*link, report func(snapshotReport)) *node {
	n := &node{
		id:      id,
		balance: initialBalance,
		events:  NewMailbox[nodeEvent](ctx),
		links:   links,
		active:  make(map[int64]*nodeSnapshotSession),
		retired: make(map[int64]bool),
		report:  report,
	}
	go n.run(ctx)
	return n
}

func (n *node) run(ctx context.Context) {
	for {
		event, ok := n.events.Recv(ctx)
		if !ok {
			return
		}
		if event.cmd.transferCmd != nil || event.cmd.begin != nil || event.cmd.retire != nil {
			n.handleCommand(event.cmd)
			continue
		}
		n.handleIngress(event.ingress)
	}
}

func (n *node) submitTransfer(ctx context.Context, t transfer) error {
	reply := make(chan error, 1)
	if !n.events.Put(nodeEvent{cmd: nodeCommand{transferCmd: &transferCmd{t: t, reply: reply}}}) {
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *node) beginSnapshot(ctx context.Context, id int64) error {
	reply := make(chan error, 1)
	if !n.events.Put(nodeEvent{cmd: nodeCommand{begin: &beginSnapshot{id: id, reply: reply}}}) {
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *node) handleCommand(cmd nodeCommand) {
	switch {
	case cmd.retire != nil:
		delete(n.active, cmd.retire.id)
		// A retired id is terminal at this node: its markers may still be
		// queued in the FIFO channels, but none of them may re-open the cut.
		n.retired[cmd.retire.id] = true
		cmd.retire.reply <- nil
	case cmd.transferCmd != nil:
		n.handleTransferCommand(cmd.transferCmd)
	case cmd.begin != nil:
		n.handleBegin(cmd.begin)
	}
}

func (n *node) handleTransferCommand(req *transferCmd) {
	t := req.t
	if n.balance < t.Amount {
		req.reply <- errInsufficientFunds
		return
	}

	// Debit and reliable enqueue are one indivisible action on this node's
	// only event loop. The destination is credited only after delivery.
	n.balance -= t.Amount
	n.links[t.To].enqueue(&envelope{transfer: &t})
	req.reply <- nil
}

func (n *node) handleBegin(req *beginSnapshot) {
	if n.retired[req.id] {
		// Late marker for an already completed/cancelled collection.
		req.reply <- nil
		return
	}
	if _, exists := n.active[req.id]; exists {
		req.reply <- nil
		return
	}

	// 1. Record the local state first.
	n.report(snapshotReport{
		nodeID:     n.id,
		kind:       reportLocal,
		snapshotID: req.id,
		balance:    n.balance,
	})

	// 2. Markers are enqueued on every outgoing channel before this event
	// loop processes any later transfer.
	session := &nodeSnapshotSession{
		id:        req.id,
		channels:  make(map[int]*snapshotChannelState),
		remaining: nodeCount - 1,
	}
	for other := 0; other < nodeCount; other++ {
		if other == n.id {
			continue
		}
		n.links[other].enqueue(&envelope{marker: &marker{SnapshotID: req.id, From: n.id}})
		session.channels[other] = &snapshotChannelState{transfers: []transfer{}}
	}
	n.active[req.id] = session
	req.reply <- nil
}

func (n *node) handleIngress(item *envelope) {
	switch {
	case item.transfer != nil:
		n.handleTransferIngress(item.transfer)
	case item.marker != nil:
		n.handleMarkerIngress(item.marker)
	}
}

func (n *node) handleTransferIngress(t *transfer) {
	n.balance += t.Amount

	for _, session := range n.active {
		// After this node's local cut but before the marker on this particular
		// incoming channel, transfers are recorded as that channel's state.
		if state, exists := session.channels[t.From]; exists && !state.closed {
			state.amount += t.Amount
			state.transfers = append(state.transfers, *t)
		}
	}
}

func (n *node) handleMarkerIngress(m *marker) {
	if n.retired[m.SnapshotID] {
		// The collection already reached a terminal state (completed or
		// cancelled). A queued, late, or duplicated marker must not regenerate
		// any state for it and must not touch any other collection.
		return
	}

	session, exists := n.active[m.SnapshotID]
	if !exists {
		// The first marker causes the local cut; handleBegin emits this node's
		// markers before the ingress event following this marker can run.
		begin := &beginSnapshot{id: m.SnapshotID, reply: make(chan error, 1)}
		n.handleBegin(begin)
		session = n.active[m.SnapshotID]
		if session == nil {
			// handleBegin refused a retired id; do not revive the collection.
			return
		}
	}

	state, ok := session.channels[m.From]
	if !ok || state.closed {
		// A duplicate/stale marker must not replace an already reported cut or
		// corrupt the remaining-channel count.
		return
	}
	n.report(snapshotReport{
		nodeID:     n.id,
		kind:       reportChannel,
		snapshotID: m.SnapshotID,
		from:       m.From,
		state:      state,
	})
	state.closed = true

	session.remaining--
	if session.remaining == 0 {
		delete(n.active, m.SnapshotID)
	}
}

var transferIDCounter atomic.Uint64

func nextTransferID() string {
	seq := transferIDCounter.Add(1)
	return "t-" + itoa(int64(seq))
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	negative := v < 0
	if negative {
		v = -v
	}
	var buf [20]byte
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func (n *node) retire(ctx context.Context, id int64) error {
	reply := make(chan error, 1)
	if !n.events.Put(nodeEvent{cmd: nodeCommand{retire: &beginSnapshot{id, reply}}}) {
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
