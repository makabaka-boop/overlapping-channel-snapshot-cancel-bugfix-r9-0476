package main

import (
	"context"
	"sync/atomic"
)

type transferView struct {
	ID     string `json:"id"`
	From   int    `json:"from"`
	To     int    `json:"to"`
	Amount int    `json:"amount"`
}

type channelSnapshot struct {
	From      int            `json:"from"`
	To        int            `json:"to"`
	Amount    int            `json:"amount"`
	Transfers []transferView `json:"transfers"`
}

type nodeSnapshotView struct {
	Node    int `json:"node"`
	Balance int `json:"balance"`
}

type pendingChannel struct {
	From int `json:"from"`
	To   int `json:"to"`
}

type pendingNode struct {
	Node int `json:"node"`
}

type snapshotResult struct {
	ID              int64              `json:"id"`
	Cancelled       bool               `json:"cancelled,omitempty"`
	Complete        bool               `json:"complete"`
	InitialTotal    int                `json:"initial_total"`
	RecordedTotal   int                `json:"recorded_total"`
	ConservationOK  bool               `json:"conservation_ok"`
	Balances        []nodeSnapshotView `json:"balances,omitempty"`
	Channels        []channelSnapshot  `json:"channels,omitempty"`
	PendingLocal    []pendingNode      `json:"pending_local,omitempty"`
	PendingChannels []pendingChannel   `json:"pending_channels,omitempty"`
}

type coordinatorEvent struct {
	cancel *coordinatorCancel
	start  *coordinatorStart
	get    *coordinatorGet
	report *snapshotReport
}

type coordinatorCancel struct {
	id    int64
	reply chan getResult
}

type coordinatorStart struct {
	initiator int
	reply     chan startResult
}

type startResult struct {
	id  int64
	err error
}

type coordinatorGet struct {
	id    int64
	reply chan getResult
}

type getResult struct {
	result *snapshotResult
	err    error
}

type coordinator struct {
	events *Mailbox[coordinatorEvent]

	nodes []*node
	links [][]*link

	overlap bool
	nextID  atomic.Int64
	active  *globalSnapshot
	history map[int64]*snapshotResult
}

type globalSnapshot struct {
	id       int64
	balances map[int]int
	channels map[channelKey]*channelSnapshot

	localDone   map[int]bool
	channelDone map[channelKey]bool
}

type channelKey struct{ from, to int }

func newCoordinator(ctx context.Context, nodes []*node, links [][]*link, overlapping ...bool) *coordinator {
	c := &coordinator{
		events:  NewMailbox[coordinatorEvent](ctx),
		nodes:   nodes,
		links:   links,
		history: make(map[int64]*snapshotResult),
	}
	if len(overlapping) > 0 {
		c.overlap = overlapping[0]
	}
	go c.run(ctx)
	return c
}

func (c *coordinator) report(report snapshotReport) {
	// Reports are serialized into the coordinator's private event loop. It
	// never reads current balances or mutates queues to fill missing pieces.
	c.events.Put(coordinatorEvent{report: &report})
}

func (c *coordinator) start(ctx context.Context, initiator int) (int64, error) {
	reply := make(chan startResult, 1)
	if !c.events.Put(coordinatorEvent{start: &coordinatorStart{initiator: initiator, reply: reply}}) {
		return 0, ctx.Err()
	}
	select {
	case result := <-reply:
		return result.id, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (c *coordinator) get(ctx context.Context, id int64) (*snapshotResult, error) {
	reply := make(chan getResult, 1)
	if !c.events.Put(coordinatorEvent{get: &coordinatorGet{id: id, reply: reply}}) {
		return nil, ctx.Err()
	}
	select {
	case result := <-reply:
		return result.result, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *coordinator) run(ctx context.Context) {
	for {
		event, ok := c.events.Recv(ctx)
		if !ok {
			return
		}
		switch {
		case event.cancel != nil:
			c.handleCancel(ctx, event.cancel)
		case event.start != nil:
			c.handleStart(ctx, event.start)
		case event.get != nil:
			c.handleGet(event.get)
		case event.report != nil:
			c.handleReport(*event.report)
		}
	}
}

func (c *coordinator) handleStart(ctx context.Context, req *coordinatorStart) {
	if c.active != nil && !c.overlap {
		req.reply <- startResult{err: errSnapshotActive}
		return
	}

	if c.active != nil {
		c.history[c.active.id] = c.active.view()
	}
	id := c.nextID.Add(1)
	c.active = &globalSnapshot{
		id:          id,
		balances:    make(map[int]int),
		channels:    make(map[channelKey]*channelSnapshot),
		localDone:   make(map[int]bool),
		channelDone: make(map[channelKey]bool),
	}

	// Starting on the chosen node both records its local state and queues its
	// markers before this synchronous call returns. Markers propagate through
	// the normal channels to every other node.
	if err := c.nodes[req.initiator].beginSnapshot(ctx, id); err != nil {
		delete(c.history, id)
		c.active = nil
		req.reply <- startResult{err: err}
		return
	}

	req.reply <- startResult{id: id}
}

func (c *coordinator) handleGet(req *coordinatorGet) {
	if c.active != nil && c.active.id == req.id {
		req.reply <- getResult{result: c.active.view()}
		return
	}
	result, exists := c.history[req.id]
	if !exists {
		req.reply <- getResult{err: errSnapshotNotFound}
		return
	}
	req.reply <- getResult{result: result}
}

func (c *coordinator) handleReport(report snapshotReport) {
	if c.active == nil || c.active.id != report.snapshotID {
		// Reports from a finished or unknown snapshot cannot contaminate a
		// later collection.
		return
	}

	s := c.active
	switch report.kind {
	case reportLocal:
		if s.localDone[report.nodeID] {
			return
		}
		s.localDone[report.nodeID] = true
		s.balances[report.nodeID] = report.balance
	case reportChannel:
		key := channelKey{from: report.from, to: report.nodeID}
		if s.channelDone[key] {
			return
		}
		s.channelDone[key] = true
		copiedTransfers := make([]transferView, 0, len(report.state.transfers))
		for _, recorded := range report.state.transfers {
			copiedTransfers = append(copiedTransfers, transferView{
				ID:     recorded.ID,
				From:   recorded.From + 1,
				To:     recorded.To + 1,
				Amount: recorded.Amount,
			})
		}
		s.channels[key] = &channelSnapshot{
			From:      report.from + 1,
			To:        report.nodeID + 1,
			Amount:    report.state.amount,
			Transfers: copiedTransfers,
		}
	}

	if len(s.localDone) == nodeCount && len(s.channelDone) == nodeCount*(nodeCount-1) {
		result := s.view()
		c.history[s.id] = result
		c.active = nil
	}
}

func (s *globalSnapshot) view() *snapshotResult {
	balances := make([]nodeSnapshotView, 0, nodeCount)
	balanceSum := 0
	for nodeID := 0; nodeID < nodeCount; nodeID++ {
		if !s.localDone[nodeID] {
			continue
		}
		balance := s.balances[nodeID]
		balances = append(balances, nodeSnapshotView{Node: nodeID + 1, Balance: balance})
		balanceSum += balance
	}

	channels := make([]channelSnapshot, 0, nodeCount*(nodeCount-1))
	channelSum := 0
	for from := 0; from < nodeCount; from++ {
		for to := 0; to < nodeCount; to++ {
			if from == to {
				continue
			}
			state, exists := s.channels[channelKey{from: from, to: to}]
			if !exists {
				continue
			}
			channels = append(channels, *state)
			channelSum += state.Amount
		}
	}

	pendingLocal := make([]pendingNode, 0)
	for nodeID := 0; nodeID < nodeCount; nodeID++ {
		if !s.localDone[nodeID] {
			pendingLocal = append(pendingLocal, pendingNode{Node: nodeID + 1})
		}
	}

	pendingChannels := make([]pendingChannel, 0)
	for from := 0; from < nodeCount; from++ {
		for to := 0; to < nodeCount; to++ {
			if from == to {
				continue
			}
			key := channelKey{from: from, to: to}
			if !s.channelDone[key] {
				pendingChannels = append(pendingChannels, pendingChannel{From: from + 1, To: to + 1})
			}
		}
	}

	complete := len(pendingLocal) == 0 && len(pendingChannels) == 0
	total := nodeCount * initialBalance
	recorded := balanceSum + channelSum
	return &snapshotResult{
		ID:              s.id,
		Complete:        complete,
		InitialTotal:    total,
		RecordedTotal:   recorded,
		ConservationOK:  complete && recorded == total,
		Balances:        balances,
		Channels:        channels,
		PendingLocal:    pendingLocal,
		PendingChannels: pendingChannels,
	}
}

func (c *coordinator) cancel(ctx context.Context, id int64) (*snapshotResult, error) {
	reply := make(chan getResult, 1)
	if !c.events.Put(coordinatorEvent{cancel: &coordinatorCancel{id, reply}}) {
		return nil, ctx.Err()
	}
	select {
	case result := <-reply:
		return result.result, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (c *coordinator) handleCancel(ctx context.Context, req *coordinatorCancel) {
	if c.active != nil && c.active.id == req.id {
		result := c.active.view()
		result.Cancelled = true
		result.Complete = false
		result.ConservationOK = false
		c.history[req.id] = result
		c.active = nil
		for _, node := range c.nodes {
			node.retire(ctx, req.id)
		}
		req.reply <- getResult{result: result}
		return
	}
	c.handleGet(&coordinatorGet{id: req.id, reply: req.reply})
}
