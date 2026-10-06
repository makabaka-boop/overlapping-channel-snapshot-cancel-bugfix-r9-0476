package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestOverlappingSnapshotsCollectIndependently starts two overlapping cuts in
// overlap mode. Each id keeps its own local cuts, in-flight channel state, and
// completion; a third start is rejected while two slots are taken.
func TestOverlappingSnapshotsCollectIndependently(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)

	hold(t, s, 2, 1)
	first := mustTransfer(t, ctx, s, 2, 1, 10, "ov-a")
	firstID := startSnapshot(t, ctx, s, 2)

	// Let the first cut reach every node (and enqueue its markers) before the
	// later transfer, so the FIFO attribution of each transfer is deterministic.
	waitAllLocalCuts(t, ctx, s, firstID)

	hold(t, s, 3, 1)
	second := mustTransfer(t, ctx, s, 3, 1, 20, "ov-b")
	// Initiate the second cut at node 1: its local cut is immediate, while the
	// held 3->1 transfer (and the still-held 2->1 transfer) each stay in front
	// of their own channel markers.
	secondID := startSnapshot(t, ctx, s, 1)
	if secondID == firstID {
		t.Fatalf("overlapping snapshot reused id %d", firstID)
	}

	// At most two active collections, in overlap mode as well.
	if _, err := s.startSnapshot(ctx, 1); err != errSnapshotActive {
		t.Fatalf("third active snapshot error = %v, want %v", err, errSnapshotActive)
	}

	// Both cuts are independently visible and unfinished while barriers hold.
	waitAllLocalCuts(t, ctx, s, secondID)
	for _, id := range []int64{firstID, secondID} {
		partial, err := s.getSnapshot(ctx, id)
		if err != nil {
			t.Fatalf("get partial %d: %v", id, err)
		}
		if partial.Complete {
			t.Fatalf("snapshot %d completed behind held barriers", id)
		}
		if partial.ConservationOK {
			t.Fatalf("incomplete snapshot %d reports conservation success", id)
		}
		if partial.Cancelled {
			t.Fatalf("snapshot %d unexpectedly cancelled", id)
		}
	}

	release(t, s, 2, 1)
	release(t, s, 3, 1)

	r1 := awaitComplete(t, ctx, s, firstID)
	r2 := awaitComplete(t, ctx, s, secondID)
	assertTotal(t, r1)
	assertTotal(t, r2)

	// First cut: node 2's debit only; the later 3->1 transfer is past its
	// marker and must not leak in.
	if balance := findBalance(r1, 2); balance != 90 {
		t.Fatalf("snapshot %d node 2 balance = %d, want 90", firstID, balance)
	}
	if balance := findBalance(r1, 3); balance != 100 {
		t.Fatalf("snapshot %d node 3 balance = %d, want 100", firstID, balance)
	}
	c1 := findChannel(t, r1, 2, 1)
	if c1.Amount != 10 || len(c1.Transfers) != 1 || c1.Transfers[0].ID != first.ID {
		t.Fatalf("snapshot %d channel 2->1 = %#v, want only %s", firstID, c1, first.ID)
	}
	if late := findChannel(t, r1, 3, 1); late.Amount != 0 || len(late.Transfers) != 0 {
		t.Fatalf("later transfer leaked into snapshot %d: %#v", firstID, late)
	}

	// Second cut records both in-flight transfers, each with its own marker.
	if balance := findBalance(r2, 1); balance != 100 {
		t.Fatalf("snapshot %d node 1 balance = %d, want 100", secondID, balance)
	}
	if balance := findBalance(r2, 2); balance != 90 {
		t.Fatalf("snapshot %d node 2 balance = %d, want 90", secondID, balance)
	}
	if balance := findBalance(r2, 3); balance != 80 {
		t.Fatalf("snapshot %d node 3 balance = %d, want 80", secondID, balance)
	}
	c2a := findChannel(t, r2, 2, 1)
	if c2a.Amount != 10 || len(c2a.Transfers) != 1 || c2a.Transfers[0].ID != first.ID {
		t.Fatalf("snapshot %d channel 2->1 = %#v, want %s", secondID, c2a, first.ID)
	}
	c2b := findChannel(t, r2, 3, 1)
	if c2b.Amount != 20 || len(c2b.Transfers) != 1 || c2b.Transfers[0].ID != second.ID {
		t.Fatalf("snapshot %d channel 3->1 = %#v, want %s", secondID, c2b, second.ID)
	}

	// Finished records are immutable and stay independently queryable.
	again1, err := s.getSnapshot(ctx, firstID)
	if err != nil {
		t.Fatalf("get first again: %v", err)
	}
	if !again1.Complete || again1.RecordedTotal != 300 {
		t.Fatalf("first snapshot mutated: %#v", again1)
	}
}

// TestCancelStopsOnlyItsSnapshot exercises cancellation against a still-active
// neighbour: partial evidence is retained as a stable cancelled terminal state,
// the slot frees, and late/duplicate markers cannot revive the dead cut.
func TestCancelStopsOnlyItsSnapshot(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)
	holdAll(t, s)
	defer releaseAll(t, s)

	id1 := startSnapshot(t, ctx, s, 1)
	id2 := startSnapshot(t, ctx, s, 2)

	// Cancel the first while it is stuck with only the initiator's local cut.
	cancelled, err := s.coord.cancel(ctx, id1)
	if err != nil {
		t.Fatalf("cancel %d: %v", id1, err)
	}
	if !cancelled.Cancelled || cancelled.Complete || cancelled.ConservationOK {
		t.Fatalf("cancel terminal = %#v", cancelled)
	}
	if len(cancelled.Balances) != 1 || cancelled.Balances[0].Node != 1 {
		t.Fatalf("cancelled evidence = %#v, want initiator local cut", cancelled.Balances)
	}
	if len(cancelled.PendingLocal) == 0 || len(cancelled.PendingChannels) == 0 {
		t.Fatalf("cancelled record lost its pending evidence: %#v", cancelled)
	}

	// GET and repeated DELETE return the same frozen terminal state.
	got1, err := s.getSnapshot(ctx, id1)
	if err != nil {
		t.Fatalf("get cancelled: %v", err)
	}
	if !sameTerminal(got1, cancelled) {
		t.Fatalf("GET after cancel changed state:\n%#v\n%#v", got1, cancelled)
	}
	again, err := s.coord.cancel(ctx, id1)
	if err != nil {
		t.Fatalf("repeat cancel: %v", err)
	}
	if !sameTerminal(again, cancelled) {
		t.Fatalf("repeat cancel changed state:\n%#v\n%#v", again, cancelled)
	}

	// Cancelling the first freed exactly one slot: a third collection is now
	// accepted while the second remains active.
	id3 := startSnapshot(t, ctx, s, 3)
	if _, err := s.startSnapshot(ctx, 1); err != errSnapshotActive {
		t.Fatalf("expected both slots taken, got %v", err)
	}

	// Cancelling an unknown id is not found.
	if _, err := s.coord.cancel(ctx, 999); err != errSnapshotNotFound {
		t.Fatalf("cancel unknown error = %v, want %v", err, errSnapshotNotFound)
	}

	// Drop the third collection as well; its late markers must be harmless.
	if r, err := s.coord.cancel(ctx, id3); err != nil || !r.Cancelled {
		t.Fatalf("cancel %d: result=%#v err=%v", id3, r, err)
	}

	// Transfers still use the nodes' own balances while collections stop.
	mustTransfer(t, ctx, s, 1, 2, 5, "ov-c-during-cancel")

	// Release every queued marker and transfer. The second snapshot must still
	// complete on its own and conserve; the dead ones must stay dead.
	releaseAll(t, s)
	completed2 := awaitComplete(t, ctx, s, id2)
	assertTotal(t, completed2)

	// Give late markers time to drain through every dispatcher, then assert the
	// cancelled terminal states were never regenerated.
	time.Sleep(20 * time.Millisecond)
	for _, id := range []int64{id1, id3} {
		terminal, err := s.getSnapshot(ctx, id)
		if err != nil {
			t.Fatalf("get dead snapshot %d: %v", id, err)
		}
		if !terminal.Cancelled || terminal.Complete || terminal.ConservationOK {
			t.Fatalf("dead snapshot %d regenerated: %#v", id, terminal)
		}
		if len(terminal.Balances) != 1 {
			t.Fatalf("dead snapshot %d evidence changed after late markers: %#v", id, terminal.Balances)
		}
		if _, err := s.coord.cancel(ctx, id); err != nil {
			t.Fatalf("late repeat cancel %d: %v", id, err)
		}
	}

	// Cancelling an already complete result returns it unchanged.
	cancelledDone, err := s.coord.cancel(ctx, id2)
	if err != nil {
		t.Fatalf("cancel completed: %v", err)
	}
	if !cancelledDone.Complete || cancelledDone.Cancelled || !cancelledDone.ConservationOK {
		t.Fatalf("completion rewritten by cancel: %#v", cancelledDone)
	}
	if cancelledDone.RecordedTotal != completed2.RecordedTotal {
		t.Fatalf("completed total changed: %d vs %d", cancelledDone.RecordedTotal, completed2.RecordedTotal)
	}
}

// TestDefaultModeStillSingle keeps the default (non-overlap) behaviour: a
// second concurrent start is rejected and cancellation frees the only slot.
func TestDefaultModeStillSingle(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)
	holdAll(t, s)
	defer releaseAll(t, s)

	id1 := startSnapshot(t, ctx, s, 1)
	if _, err := s.startSnapshot(ctx, 2); err != errSnapshotActive {
		t.Fatalf("second active snapshot error = %v, want %v", err, errSnapshotActive)
	}

	if r, err := s.coord.cancel(ctx, id1); err != nil || !r.Cancelled {
		t.Fatalf("cancel default snapshot: %#v %v", r, err)
	}

	id2 := startSnapshot(t, ctx, s, 2)
	releaseAll(t, s)
	assertTotal(t, awaitComplete(t, ctx, s, id2))
}

// TestDuplicateMarkerDoesNotCorruptSession injects a second marker for an
// already closed incoming channel while that node is still waiting on another
// channel. The duplicate must not double-report, delete the session early, or
// disturb the still-running cut.
func TestDuplicateMarkerDoesNotCorruptSession(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)

	// Hold node 3 -> node 1. Node 1 still receives its marker from node 2 and
	// closes that incoming channel while remaining open for 3 -> 1.
	hold(t, s, 3, 1)
	id := startSnapshot(t, ctx, s, 3)
	waitAllLocalCuts(t, ctx, s, id)

	// Duplicate marker for the already closed 2 -> 1 channel (0-based 1 -> 0).
	s.links[1][0].enqueue(&envelope{marker: &marker{SnapshotID: id, From: 1}})
	time.Sleep(20 * time.Millisecond)

	release(t, s, 3, 1)
	assertTotal(t, awaitComplete(t, ctx, s, id))
}

func sameTerminal(a, b *snapshotResult) bool {
	return a.ID == b.ID &&
		a.Cancelled == b.Cancelled &&
		a.Complete == b.Complete &&
		a.ConservationOK == b.ConservationOK &&
		a.RecordedTotal == b.RecordedTotal &&
		len(a.Balances) == len(b.Balances) &&
		len(a.Channels) == len(b.Channels) &&
		len(a.PendingLocal) == len(b.PendingLocal) &&
		len(a.PendingChannels) == len(b.PendingChannels)
}

// --- Real HTTP (TCP listener + http.Client) end-to-end coverage ---

type httpClient struct {
	t      *testing.T
	base   string
	client *http.Client
}

func newHTTPClient(t *testing.T, s *system) *httpClient {
	t.Helper()
	server := httptest.NewServer(newHTTPServer(s))
	t.Cleanup(server.Close)
	return &httpClient{t: t, base: server.URL, client: server.Client()}
}

func (h *httpClient) do(method, path string, body any, out any) (int, []byte) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, h.base+path, reader)
	if err != nil {
		h.t.Fatalf("request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("read body: %v", err)
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			h.t.Fatalf("decode %s: %v body=%s", path, err, data)
		}
	}
	return resp.StatusCode, data
}

func httpAwaitStatus(t *testing.T, h *httpClient, id int64, status int) snapshotResult {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		var result snapshotResult
		code, _ := h.do(http.MethodGet, "/snapshots/"+itoa(id), nil, &result)
		if code == status {
			return result
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot %d never reached HTTP %d", id, status)
	return snapshotResult{}
}

func httpAwaitLocalCuts(t *testing.T, h *httpClient, id int64) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		var result snapshotResult
		h.do(http.MethodGet, "/snapshots/"+itoa(id), nil, &result)
		if len(result.Balances) == nodeCount {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot %d never recorded all local cuts over HTTP", id)
}

func TestHTTPOverlappingSnapshots(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)
	h := newHTTPClient(t, s)

	h.do(http.MethodPut, "/test/barriers", barrierRequest{From: 2, To: 1, Held: true}, nil)
	var created transferResponse
	if code, _ := h.do(http.MethodPost, "/transfers",
		transferRequest{ID: "http-ov-a", From: 2, To: 1, Amount: 10}, &created); code != http.StatusCreated {
		t.Fatalf("transfer status = %d, want 201", code)
	}

	var first snapshotResult
	if code, _ := h.do(http.MethodPost, "/snapshots", map[string]int{"initiator": 2}, &first); code != http.StatusAccepted {
		t.Fatalf("first start status = %d, want 202", code)
	}
	httpAwaitLocalCuts(t, h, first.ID)

	h.do(http.MethodPut, "/test/barriers", barrierRequest{From: 3, To: 1, Held: true}, nil)
	if code, _ := h.do(http.MethodPost, "/transfers",
		transferRequest{ID: "http-ov-b", From: 3, To: 1, Amount: 20}, &created); code != http.StatusCreated {
		t.Fatalf("second transfer status = %d", code)
	}

	var second snapshotResult
	if code, _ := h.do(http.MethodPost, "/snapshots", map[string]int{"initiator": 1}, &second); code != http.StatusAccepted {
		t.Fatalf("second start status = %d, want 202", code)
	}
	if second.ID == first.ID {
		t.Fatalf("snapshot ids collide: %d", first.ID)
	}

	// Third concurrent collection is rejected over real HTTP.
	code, body := h.do(http.MethodPost, "/snapshots", map[string]int{"initiator": 3}, nil)
	if code != http.StatusConflict {
		t.Fatalf("third start status = %d body=%s, want 409", code, body)
	}

	httpAwaitLocalCuts(t, h, second.ID)

	h.do(http.MethodPut, "/test/barriers", barrierRequest{From: 2, To: 1, Held: false}, nil)
	h.do(http.MethodPut, "/test/barriers", barrierRequest{From: 3, To: 1, Held: false}, nil)

	done1 := httpAwaitStatus(t, h, first.ID, http.StatusOK)
	done2 := httpAwaitStatus(t, h, second.ID, http.StatusOK)
	for _, r := range []snapshotResult{done1, done2} {
		if !r.Complete || !r.ConservationOK || r.RecordedTotal != 300 {
			t.Fatalf("completed snapshot bad: %#v", r)
		}
	}
	if c := findChannel(t, &done1, 3, 1); c.Amount != 0 || len(c.Transfers) != 0 {
		t.Fatalf("first snapshot leaked later transfer: %#v", c)
	}
	if c := findChannel(t, &done2, 3, 1); c.Amount != 20 || len(c.Transfers) != 1 || c.Transfers[0].ID != "http-ov-b" {
		t.Fatalf("second snapshot 3->1 = %#v", c)
	}
	if c := findChannel(t, &done2, 2, 1); c.Amount != 10 || c.Transfers[0].ID != "http-ov-a" {
		t.Fatalf("second snapshot 2->1 = %#v", c)
	}
}

func TestHTTPCancelLifecycle(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)
	h := newHTTPClient(t, s)

	// Hold every dispatcher so neither collection can finish.
	for from := 1; from <= nodeCount; from++ {
		for to := 1; to <= nodeCount; to++ {
			if from != to {
				h.do(http.MethodPut, "/test/barriers", barrierRequest{From: from, To: to, Held: true}, nil)
			}
		}
	}

	var first snapshotResult
	if code, _ := h.do(http.MethodPost, "/snapshots", map[string]int{"initiator": 1}, &first); code != http.StatusAccepted {
		t.Fatalf("first start status = %d", code)
	}
	var second snapshotResult
	h.do(http.MethodPost, "/snapshots", map[string]int{"initiator": 2}, &second)

	var cancelled snapshotResult
	if code, _ := h.do(http.MethodDelete, "/snapshots/"+itoa(first.ID), nil, &cancelled); code != http.StatusOK {
		t.Fatalf("DELETE active status = %d", code)
	}
	if !cancelled.Cancelled || cancelled.Complete || cancelled.ConservationOK {
		t.Fatalf("DELETE response not a cancelled terminal: %#v", cancelled)
	}

	// GET and repeated DELETE return the same stable terminal state over HTTP.
	var viaGet snapshotResult
	if code, _ := h.do(http.MethodGet, "/snapshots/"+itoa(first.ID), nil, &viaGet); code != http.StatusOK {
		t.Fatalf("GET cancelled status = %d, want 200", code)
	}
	if !viaGet.Cancelled || viaGet.Complete || viaGet.ConservationOK || !sameTerminal(&viaGet, &cancelled) {
		t.Fatalf("GET terminal differs: %#v vs %#v", viaGet, cancelled)
	}
	var viaDelete snapshotResult
	if code, _ := h.do(http.MethodDelete, "/snapshots/"+itoa(first.ID), nil, &viaDelete); code != http.StatusOK {
		t.Fatalf("repeat DELETE status = %d", code)
	}
	if !sameTerminal(&viaDelete, &cancelled) {
		t.Fatalf("repeat DELETE terminal differs: %#v vs %#v", viaDelete, cancelled)
	}

	// Slot released; the third start is accepted while the second is active.
	var third snapshotResult
	if code, _ := h.do(http.MethodPost, "/snapshots", map[string]int{"initiator": 3}, &third); code != http.StatusAccepted {
		t.Fatalf("start after cancel status = %d, want 202", code)
	}
	h.do(http.MethodDelete, "/snapshots/"+itoa(third.ID), nil, nil)

	// Unknown id -> 404.
	if code, _ := h.do(http.MethodDelete, "/snapshots/4242", nil, nil); code != http.StatusNotFound {
		t.Fatalf("DELETE unknown status = %d, want 404", code)
	}

	// A transfer still moves using live balances.
	if code, _ := h.do(http.MethodPost, "/transfers",
		transferRequest{ID: "http-ov-c", From: 1, To: 2, Amount: 7}, nil); code != http.StatusCreated {
		t.Fatalf("transfer during cancellation status = %d", code)
	}

	for from := 1; from <= nodeCount; from++ {
		for to := 1; to <= nodeCount; to++ {
			if from != to {
				h.do(http.MethodPut, "/test/barriers", barrierRequest{From: from, To: to, Held: false}, nil)
			}
		}
	}

	// The neighbour collection completes independently, transfer by transfer.
	done := httpAwaitStatus(t, h, second.ID, http.StatusOK)
	if !done.Complete || !done.ConservationOK || done.RecordedTotal != nodeCount*initialBalance {
		t.Fatalf("neighbour snapshot bad: %#v", done)
	}
	var channelSum int
	for _, ch := range done.Channels {
		channelAmount := 0
		for _, xfer := range ch.Transfers {
			channelAmount += xfer.Amount
			channelSum += xfer.Amount
			if xfer.From != ch.From || xfer.To != ch.To {
				t.Fatalf("transfer %s does not trace its channel %d->%d", xfer.ID, ch.From, ch.To)
			}
		}
		if ch.Amount != channelAmount {
			t.Fatalf("channel %d->%d amount %d disagrees with transfers %v", ch.From, ch.To, ch.Amount, ch.Transfers)
		}
	}
	var balanceSum int
	for _, b := range done.Balances {
		balanceSum += b.Balance
	}
	if balanceSum+channelSum != nodeCount*initialBalance {
		t.Fatalf("per-entry total %d+%d != %d", balanceSum, channelSum, nodeCount*initialBalance)
	}

	// Late markers drained; the cancelled collection never revives.
	time.Sleep(20 * time.Millisecond)
	var dead snapshotResult
	if code, _ := h.do(http.MethodGet, "/snapshots/"+itoa(first.ID), nil, &dead); code != http.StatusOK {
		t.Fatalf("GET dead status = %d", code)
	}
	if !dead.Cancelled || dead.Complete || dead.ConservationOK {
		t.Fatalf("dead snapshot regenerated over HTTP: %#v", dead)
	}

	// DELETE on the completed snapshot leaves it intact.
	var deleteDone snapshotResult
	if code, _ := h.do(http.MethodDelete, "/snapshots/"+itoa(second.ID), nil, &deleteDone); code != http.StatusOK {
		t.Fatalf("DELETE completed status = %d", code)
	}
	if !deleteDone.Complete || deleteDone.Cancelled || !deleteDone.ConservationOK {
		t.Fatalf("completed result rewritten by DELETE: %#v", deleteDone)
	}

	// Sanity: health keeps working.
	if code, _ := h.do(http.MethodGet, "/health", nil, nil); code != http.StatusOK {
		t.Fatalf("health status = %d", code)
	}
}
