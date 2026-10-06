package main

import (
	"context"
	"sync"
)

type linkStatusView struct {
	From        int  `json:"from"`
	To          int  `json:"to"`
	Held        bool `json:"held"`
	QueuedItems int  `json:"queued_items"`
}

type barrierRequest struct {
	From int  `json:"from"`
	To   int  `json:"to"`
	Held bool `json:"held"`
}

type system struct {
	nodes []*node
	links [][]*link
	coord *coordinator

	transferMu sync.Mutex
	knownIDs   map[string]struct{}
}

func newSystem(ctx context.Context, overlap ...bool) *system {
	links := make([][]*link, nodeCount)
	for from := 0; from < nodeCount; from++ {
		links[from] = make([]*link, nodeCount)
		for to := 0; to < nodeCount; to++ {
			if from == to {
				continue
			}
			links[from][to] = newLink(ctx, from, to)
		}
	}

	s := &system{knownIDs: make(map[string]struct{}), links: links}
	nodes := make([]*node, nodeCount)
	for id := 0; id < nodeCount; id++ {
		outgoing := make(map[int]*link, nodeCount-1)
		for to, l := range links[id] {
			if l != nil {
				outgoing[to] = l
			}
		}
		nodes[id] = newNode(ctx, id, outgoing, func(report snapshotReport) {
			if s.coord != nil {
				s.coord.report(report)
			}
		})
	}
	s.nodes = nodes

	coord := newCoordinator(ctx, nodes, links, overlap...)
	s.coord = coord

	// Start dispatchers after all nodes and coordinator exist. There is one
	// goroutine per directed channel, which is the reliable FIFO boundary.
	for from := range links {
		for to, l := range links[from] {
			if l != nil {
				go l.dispatch(ctx, nodes[to].events)
			}
		}
	}

	return s
}

type transferRequest struct {
	ID     string `json:"id"`
	From   int    `json:"from"`
	To     int    `json:"to"`
	Amount int    `json:"amount"`
}

type transferResponse struct {
	ID     string `json:"id"`
	From   int    `json:"from"`
	To     int    `json:"to"`
	Amount int    `json:"amount"`
}

func (s *system) submitTransfer(ctx context.Context, req transferRequest) (transferResponse, error) {
	if req.From < 1 || req.From > nodeCount || req.To < 1 || req.To > nodeCount {
		return transferResponse{}, errInvalidNode
	}
	if req.From == req.To {
		return transferResponse{}, errInvalidNode
	}
	if req.Amount <= 0 {
		return transferResponse{}, errInvalidAmount
	}

	id := req.ID
	if id == "" {
		id = nextTransferID()
	}

	s.transferMu.Lock()
	if _, exists := s.knownIDs[id]; exists {
		s.transferMu.Unlock()
		return transferResponse{}, errDuplicateTransfer
	}
	// Reserve while the node's private event loop performs the atomic
	// debit/enqueue. The reservation is released only if that action fails;
	// successful transfers retain their unique correlation ID permanently.
	s.knownIDs[id] = struct{}{}
	s.transferMu.Unlock()

	t := transfer{ID: id, From: req.From - 1, To: req.To - 1, Amount: req.Amount}
	if err := s.nodes[t.From].submitTransfer(ctx, t); err != nil {
		s.transferMu.Lock()
		delete(s.knownIDs, id)
		s.transferMu.Unlock()
		return transferResponse{}, err
	}

	return transferResponse{ID: id, From: req.From, To: req.To, Amount: req.Amount}, nil
}

func (s *system) startSnapshot(ctx context.Context, initiator int) (int64, error) {
	if initiator < 1 || initiator > nodeCount {
		return 0, errInvalidNode
	}
	return s.coord.start(ctx, initiator-1)
}

func (s *system) getSnapshot(ctx context.Context, id int64) (*snapshotResult, error) {
	return s.coord.get(ctx, id)
}

func (s *system) setBarrier(req barrierRequest) (linkStatusView, error) {
	if req.From < 1 || req.From > nodeCount || req.To < 1 || req.To > nodeCount || req.From == req.To {
		return linkStatusView{}, errInvalidNode
	}
	l := s.links[req.From-1][req.To-1]
	l.setHeld(req.Held)
	return l.snapshot(), nil
}

func (s *system) barriers() []linkStatusView {
	views := make([]linkStatusView, 0, nodeCount*(nodeCount-1))
	for from := range s.links {
		for _, l := range s.links[from] {
			if l != nil {
				views = append(views, l.snapshot())
			}
		}
	}
	return views
}
