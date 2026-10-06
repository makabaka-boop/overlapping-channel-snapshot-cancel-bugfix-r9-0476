package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

func testContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), testTimeout)
}

func mustTransfer(t *testing.T, ctx context.Context, s *system, from, to, amount int, id string) transfer {
	t.Helper()
	response, err := s.submitTransfer(ctx, transferRequest{ID: id, From: from, To: to, Amount: amount})
	if err != nil {
		t.Fatalf("transfer %d->%d amount=%d: %v", from, to, amount, err)
	}
	return transfer{ID: response.ID, From: from, To: to, Amount: amount}
}

func hold(t *testing.T, s *system, from, to int) {
	t.Helper()
	if _, err := s.setBarrier(barrierRequest{From: from, To: to, Held: true}); err != nil {
		t.Fatalf("hold %d->%d: %v", from, to, err)
	}
}

func release(t *testing.T, s *system, from, to int) {
	t.Helper()
	if _, err := s.setBarrier(barrierRequest{From: from, To: to, Held: false}); err != nil {
		t.Fatalf("release %d->%d: %v", from, to, err)
	}
}

func holdAll(t *testing.T, s *system) {
	t.Helper()
	for from := 1; from <= nodeCount; from++ {
		for to := 1; to <= nodeCount; to++ {
			if from != to {
				hold(t, s, from, to)
			}
		}
	}
}

func releaseAll(t *testing.T, s *system) {
	t.Helper()
	for from := 1; from <= nodeCount; from++ {
		for to := 1; to <= nodeCount; to++ {
			if from != to {
				release(t, s, from, to)
			}
		}
	}
}

func startSnapshot(t *testing.T, ctx context.Context, s *system, initiator int) int64 {
	t.Helper()
	id, err := s.startSnapshot(ctx, initiator)
	if err != nil {
		t.Fatalf("start snapshot at node %d: %v", initiator, err)
	}
	return id
}

func awaitComplete(t *testing.T, ctx context.Context, s *system, id int64) *snapshotResult {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		result, err := s.getSnapshot(ctx, id)
		if err != nil {
			t.Fatalf("get snapshot %d: %v", id, err)
		}
		if result.Complete {
			return result
		}
		select {
		case <-ctx.Done():
			t.Fatalf("snapshot %d did not complete: %v", id, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatalf("snapshot %d did not complete", id)
	return nil
}

func waitAllLocalCuts(t *testing.T, ctx context.Context, s *system, id int64) *snapshotResult {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	var partial *snapshotResult
	var err error
	for time.Now().Before(deadline) {
		partial, err = s.getSnapshot(ctx, id)
		if err != nil {
			t.Fatalf("get partial snapshot %d: %v", id, err)
		}
		if len(partial.Balances) == nodeCount {
			return partial
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for local cuts: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatalf("snapshot %d did not record all local cuts", id)
	return nil
}

func assertTotal(t *testing.T, result *snapshotResult) {
	t.Helper()
	if !result.Complete {
		t.Fatalf("snapshot %d is incomplete: local=%v channels=%v", result.ID, result.PendingLocal, result.PendingChannels)
	}
	if result.RecordedTotal != nodeCount*initialBalance {
		t.Fatalf("snapshot %d total = %d, want %d", result.ID, result.RecordedTotal, nodeCount*initialBalance)
	}
	if !result.ConservationOK {
		t.Fatalf("snapshot %d conservation flag is false", result.ID)
	}
}

func findChannel(t *testing.T, result *snapshotResult, from, to int) channelSnapshot {
	t.Helper()
	for _, channel := range result.Channels {
		if channel.From == from && channel.To == to {
			return channel
		}
	}
	t.Fatalf("missing channel %d->%d in snapshot %d", from, to, result.ID)
	return channelSnapshot{}
}

func findBalance(result *snapshotResult, node int) int {
	for _, item := range result.Balances {
		if item.Node == node {
			return item.Balance
		}
	}
	return -1
}

func hasPendingChannel(result *snapshotResult, from, to int) bool {
	for _, pending := range result.PendingChannels {
		if pending.From == from && pending.To == to {
			return true
		}
	}
	return false
}

func assertBalances(t *testing.T, result *snapshotResult, want map[int]int) {
	t.Helper()
	if len(result.Balances) != len(want) {
		t.Fatalf("snapshot %d balances = %#v, want %v", result.ID, result.Balances, want)
	}
	for node, balance := range want {
		if got := findBalance(result, node); got != balance {
			t.Fatalf("snapshot %d node %d balance = %d, want %d", result.ID, node, got, balance)
		}
	}
}

func assertChannelTrace(t *testing.T, result *snapshotResult, from, to, amount int, ids ...string) {
	t.Helper()
	channel := findChannel(t, result, from, to)
	if channel.Amount != amount {
		t.Fatalf("snapshot %d channel %d->%d amount = %d, want %d", result.ID, from, to, channel.Amount, amount)
	}
	if len(channel.Transfers) != len(ids) {
		t.Fatalf("snapshot %d channel %d->%d transfers = %#v, want ids %v", result.ID, from, to, channel.Transfers, ids)
	}
	for i, id := range ids {
		if channel.Transfers[i].ID != id {
			t.Fatalf("snapshot %d channel %d->%d transfer %d = %q, want %q", result.ID, from, to, i, channel.Transfers[i].ID, id)
		}
	}
}

// assertPerTransferReconciliation recomputes the global total from the
// recorded balances plus every individually traced in-flight transfer.
func assertPerTransferReconciliation(t *testing.T, result *snapshotResult) {
	t.Helper()
	sum := 0
	for _, balance := range result.Balances {
		sum += balance.Balance
	}
	for _, channel := range result.Channels {
		traced := 0
		for _, tracedTransfer := range channel.Transfers {
			traced += tracedTransfer.Amount
		}
		if traced != channel.Amount {
			t.Fatalf("snapshot %d channel %d->%d amount %d != traced transfer sum %d",
				result.ID, channel.From, channel.To, channel.Amount, traced)
		}
		sum += channel.Amount
	}
	if sum != nodeCount*initialBalance {
		t.Fatalf("snapshot %d reconciled total = %d, want %d", result.ID, sum, nodeCount*initialBalance)
	}
	if result.RecordedTotal != sum {
		t.Fatalf("snapshot %d recorded_total = %d, reconciled %d", result.ID, result.RecordedTotal, sum)
	}
}

func TestSnapshotIncludesDebitedButNotReceivedTransfer(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)

	hold(t, s, 1, 2)
	inFlight := mustTransfer(t, ctx, s, 1, 2, 30, "")

	id := startSnapshot(t, ctx, s, 1)

	// While the barrier is up, return the real unfinished state rather than a
	// fabricated complete snapshot.
	partial, err := s.getSnapshot(ctx, id)
	if err != nil {
		t.Fatalf("get partial snapshot: %v", err)
	}
	if partial.Complete {
		t.Fatalf("snapshot unexpectedly completed behind held barrier")
	}
	if len(partial.PendingChannels) == 0 {
		t.Fatalf("expected pending channels, got none")
	}

	// Other marker paths give node 2 its cut while the 1->2 transfer is still
	// withheld behind that channel's barrier.
	waitAllLocalCuts(t, ctx, s, id)

	release(t, s, 1, 2)
	result := awaitComplete(t, ctx, s, id)
	assertTotal(t, result)

	if balance := findBalance(result, 1); balance != 70 {
		t.Fatalf("node 1 cut balance = %d, want 70", balance)
	}
	channel := findChannel(t, result, 1, 2)
	if channel.Amount != 30 {
		t.Fatalf("channel 1->2 amount = %d, want 30", channel.Amount)
	}
	if len(channel.Transfers) != 1 || channel.Transfers[0].ID != inFlight.ID {
		t.Fatalf("channel trace = %#v, want transfer %s", channel.Transfers, inFlight.ID)
	}
}

func TestMarkersInterleavedWithTransfers(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)

	// One held outgoing channel creates the exact queue order:
	// pre-marker transfer, marker, post-marker transfer.
	hold(t, s, 2, 1)
	beforeMarker := mustTransfer(t, ctx, s, 2, 1, 10, "")
	id := startSnapshot(t, ctx, s, 2)
	afterMarker := mustTransfer(t, ctx, s, 2, 1, 20, "")

	partial, err := s.getSnapshot(ctx, id)
	if err != nil {
		t.Fatalf("get partial snapshot: %v", err)
	}
	if partial.Complete {
		t.Fatalf("snapshot unexpectedly completed behind held barrier")
	}

	// Marker propagation gives node 1 its cut while the direct 2->1 messages
	// remain ordered behind the held channel barrier.
	waitAllLocalCuts(t, ctx, s, id)
	release(t, s, 2, 1)
	result := awaitComplete(t, ctx, s, id)
	assertTotal(t, result)

	if balance := findBalance(result, 2); balance != 90 {
		t.Fatalf("node 2 cut balance = %d, want 90 (pre-marker debit only)", balance)
	}
	channel := findChannel(t, result, 2, 1)
	if channel.Amount != 10 || len(channel.Transfers) != 1 || channel.Transfers[0].ID != beforeMarker.ID {
		t.Fatalf("channel 2->1 = %#v, want only pre-marker transfer %s", channel, beforeMarker.ID)
	}

	for _, recorded := range result.Channels {
		for _, traced := range recorded.Transfers {
			if traced.ID == afterMarker.ID {
				t.Fatalf("post-marker transfer %s leaked into snapshot channel %d->%d", afterMarker.ID, recorded.From, recorded.To)
			}
		}
	}
}

func TestConsecutiveSnapshotsDoNotMix(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)

	firstTransfer := mustTransfer(t, ctx, s, 1, 3, 25, "")
	firstID := startSnapshot(t, ctx, s, 3)
	first := awaitComplete(t, ctx, s, firstID)
	assertTotal(t, first)

	// Keep an immutable copy after completion, then create a new distinct cut.
	firstCopy := *first

	hold(t, s, 2, 1)
	secondTransfer := mustTransfer(t, ctx, s, 2, 1, 15, "")
	secondID := startSnapshot(t, ctx, s, 2)

	// The completed first snapshot remains exactly as recorded and cannot be
	// overwritten by the second activity.
	againFirst, err := s.getSnapshot(ctx, firstID)
	if err != nil {
		t.Fatalf("get first snapshot again: %v", err)
	}
	if !againFirst.Complete || againFirst.RecordedTotal != firstCopy.RecordedTotal {
		t.Fatalf("first snapshot changed: %#v", againFirst)
	}
	if secondID == firstID {
		t.Fatalf("second snapshot reused id %d", firstID)
	}

	// Give node 1 its cut via another marker path before releasing the held
	// transfer. Otherwise the transfer can legitimately arrive before that
	// node's first marker and belong in its local balance rather than the cut
	// channel state.
	waitAllLocalCuts(t, ctx, s, secondID)

	release(t, s, 2, 1)
	second := awaitComplete(t, ctx, s, secondID)
	assertTotal(t, second)
	channel := findChannel(t, second, 2, 1)
	if channel.Amount != 15 || len(channel.Transfers) != 1 || channel.Transfers[0].ID != secondTransfer.ID {
		t.Fatalf("second snapshot channel = %#v, want only %s", channel, secondTransfer.ID)
	}

	firstChannel := findChannel(t, second, 1, 3)
	if firstChannel.Amount != 0 {
		t.Fatalf("old in-flight transfer %s leaked into second snapshot", firstTransfer.ID)
	}
}

func TestTransferValidation(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)

	cases := []struct {
		name string
		req  transferRequest
		want error
	}{
		{"zero amount", transferRequest{From: 1, To: 2, Amount: 0}, errInvalidAmount},
		{"negative amount", transferRequest{From: 1, To: 2, Amount: -1}, errInvalidAmount},
		{"same node", transferRequest{From: 1, To: 1, Amount: 1}, errInvalidNode},
		{"low source", transferRequest{From: 0, To: 2, Amount: 1}, errInvalidNode},
		{"high destination", transferRequest{From: 1, To: 4, Amount: 1}, errInvalidNode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.submitTransfer(ctx, tc.req); err != tc.want {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}

	hold(t, s, 2, 1)
	defer release(t, s, 2, 1)
	for i := 0; i < initialBalance; i += 10 {
		mustTransfer(t, ctx, s, 2, 1, 10, "")
	}
	if _, err := s.submitTransfer(ctx, transferRequest{From: 2, To: 1, Amount: 1}); err != errInsufficientFunds {
		t.Fatalf("overdraft error = %v, want %v", err, errInsufficientFunds)
	}

	_, err := s.submitTransfer(ctx, transferRequest{ID: "same", From: 1, To: 2, Amount: 5})
	if err != nil {
		t.Fatalf("first transfer: %v", err)
	}
	if _, err := s.submitTransfer(ctx, transferRequest{ID: "same", From: 1, To: 3, Amount: 5}); err != errDuplicateTransfer {
		t.Fatalf("duplicate transfer error = %v, want %v", err, errDuplicateTransfer)
	}
}

func TestOnlyOneActiveSnapshot(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)
	holdAll(t, s)
	defer releaseAll(t, s)

	firstID := startSnapshot(t, ctx, s, 1)
	if _, err := s.startSnapshot(ctx, 2); err != errSnapshotActive {
		t.Fatalf("second active snapshot error = %v, want %v", err, errSnapshotActive)
	}

	releaseAll(t, s)
	awaitComplete(t, ctx, s, firstID)

	nextID := startSnapshot(t, ctx, s, 1)
	awaitComplete(t, ctx, s, nextID)
}

func TestHTTPTransfersSnapshotsAndBarriers(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)
	handler := newHTTPServer(s)

	// Hold via the HTTP control API.
	putJSON(t, handler, http.MethodPut, "/test/barriers", barrierRequest{From: 1, To: 2, Held: true})

	var created transferResponse
	postJSON(t, handler, http.MethodPost, "/transfers", transferRequest{ID: "http-t1", From: 1, To: 2, Amount: 10}, http.StatusCreated, &created)
	if created.ID != "http-t1" {
		t.Fatalf("created id = %q", created.ID)
	}

	errorBody := postJSONRaw(t, handler, http.MethodPost, "/transfers", transferRequest{ID: "http-t1", From: 2, To: 3, Amount: 1})
	if errorBody.Code != http.StatusConflict {
		t.Fatalf("duplicate transfer status = %d, want 409", errorBody.Code)
	}

	var started snapshotResult
	postJSON(t, handler, http.MethodPost, "/snapshots", map[string]int{"initiator": 1}, http.StatusAccepted, &started)
	if started.Complete {
		t.Fatalf("snapshot completed while barrier held")
	}

	putJSON(t, handler, http.MethodPut, "/test/barriers", barrierRequest{From: 1, To: 2, Held: false})
	var completed snapshotResult
	awaitHTTPComplete(t, handler, started.ID, &completed)
	if completed.RecordedTotal != nodeCount*initialBalance {
		t.Fatalf("HTTP snapshot total = %d", completed.RecordedTotal)
	}
}

type rawResponse struct {
	Code int
	Body []byte
}

func postJSONRaw(t *testing.T, handler http.Handler, method, path string, body any) rawResponse {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return rawResponse{Code: recorder.Code, Body: bytes.Clone(recorder.Body.Bytes())}
}

func postJSON(t *testing.T, handler http.Handler, method, path string, body any, wantStatus int, target any) {
	t.Helper()
	response := postJSONRaw(t, handler, method, path, body)
	if response.Code != wantStatus {
		t.Fatalf("%s %s status = %d body=%s, want %d", method, path, response.Code, response.Body, wantStatus)
	}
	if target != nil {
		if err := json.Unmarshal(response.Body, target); err != nil {
			t.Fatalf("decode response: %v body=%s", err, response.Body)
		}
	}
}

func putJSON(t *testing.T, handler http.Handler, method, path string, body any) {
	t.Helper()
	postJSON(t, handler, method, path, body, http.StatusOK, nil)
}

func awaitHTTPComplete(t *testing.T, handler http.Handler, id int64, target *snapshotResult) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/snapshots/"+itoa(id), nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code == http.StatusOK {
			if err := json.Unmarshal(recorder.Body.Bytes(), target); err != nil {
				t.Fatalf("decode snapshot: %v", err)
			}
			if target.Complete {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("HTTP snapshot %d did not complete", id)
}

// Two overlapping collections track their own markers independently while a
// held channel stalls both; cancelling the first frees its slot, keeps its
// partial evidence terminal, and never disturbs the second collection.
func TestOverlappingSnapshotsCollectIndependently(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)

	hold(t, s, 1, 2)
	hold(t, s, 2, 3)

	first := mustTransfer(t, ctx, s, 1, 2, 30, "")
	idA := startSnapshot(t, ctx, s, 1)
	waitAllLocalCuts(t, ctx, s, idA)

	second := mustTransfer(t, ctx, s, 2, 3, 20, "")
	idB := startSnapshot(t, ctx, s, 1)
	waitAllLocalCuts(t, ctx, s, idB)

	// Both collections pend on the held channels; neither may claim
	// conservation while incomplete.
	for _, id := range []int64{idA, idB} {
		partial, err := s.getSnapshot(ctx, id)
		if err != nil {
			t.Fatalf("get snapshot %d: %v", id, err)
		}
		if partial.Complete || partial.ConservationOK {
			t.Fatalf("snapshot %d complete=%v conservation=%v while held", id, partial.Complete, partial.ConservationOK)
		}
		if !hasPendingChannel(partial, 1, 2) || !hasPendingChannel(partial, 2, 3) {
			t.Fatalf("snapshot %d pending = %v, want held channels 1->2 and 2->3", id, partial.PendingChannels)
		}
	}

	// A third collection exceeds the two active slots.
	if _, err := s.startSnapshot(ctx, 2); err != errSnapshotActive {
		t.Fatalf("third snapshot error = %v, want %v", err, errSnapshotActive)
	}

	// Cancelling the first keeps its accepted evidence as a stable terminal
	// state and frees its slot.
	cancelledA, err := s.coord.cancel(ctx, idA)
	if err != nil {
		t.Fatalf("cancel snapshot %d: %v", idA, err)
	}
	if !cancelledA.Cancelled || cancelledA.Complete || cancelledA.ConservationOK {
		t.Fatalf("cancelled snapshot = %#v", cancelledA)
	}
	assertBalances(t, cancelledA, map[int]int{1: 70, 2: 100, 3: 100})
	if cancelledA.RecordedTotal != 270 {
		t.Fatalf("cancelled snapshot recorded_total = %d, want 270", cancelledA.RecordedTotal)
	}
	if !hasPendingChannel(cancelledA, 1, 2) || !hasPendingChannel(cancelledA, 2, 3) {
		t.Fatalf("cancelled snapshot pending = %v, want held channels", cancelledA.PendingChannels)
	}

	// A repeated cancel returns the identical terminal state.
	again, err := s.coord.cancel(ctx, idA)
	if err != nil {
		t.Fatalf("repeated cancel: %v", err)
	}
	if !reflect.DeepEqual(again, cancelledA) {
		t.Fatalf("repeated cancel = %#v, want %#v", again, cancelledA)
	}

	// The freed slot admits a new collection while the second still waits.
	idC := startSnapshot(t, ctx, s, 3)
	waitAllLocalCuts(t, ctx, s, idC)

	// Transfers keep flowing on open channels during the collections.
	extra := mustTransfer(t, ctx, s, 1, 3, 10, "")

	release(t, s, 2, 3)
	release(t, s, 1, 2)

	resultB := awaitComplete(t, ctx, s, idB)
	assertTotal(t, resultB)
	assertPerTransferReconciliation(t, resultB)
	assertBalances(t, resultB, map[int]int{1: 70, 2: 80, 3: 100})
	assertChannelTrace(t, resultB, 1, 2, 30, first.ID)
	assertChannelTrace(t, resultB, 2, 3, 20, second.ID)

	resultC := awaitComplete(t, ctx, s, idC)
	assertTotal(t, resultC)
	assertPerTransferReconciliation(t, resultC)
	assertBalances(t, resultC, map[int]int{1: 70, 2: 80, 3: 100})
	assertChannelTrace(t, resultC, 1, 2, 30, first.ID)
	assertChannelTrace(t, resultC, 2, 3, 20, second.ID)

	for _, result := range []*snapshotResult{resultB, resultC} {
		for _, channel := range result.Channels {
			for _, traced := range channel.Transfers {
				if traced.ID == extra.ID {
					t.Fatalf("post-marker transfer %s leaked into snapshot %d channel %d->%d",
						extra.ID, result.ID, channel.From, channel.To)
				}
			}
		}
	}

	// The cancelled terminal state is untouched by the other completions and
	// by the late markers the released barriers delivered.
	finalA, err := s.getSnapshot(ctx, idA)
	if err != nil {
		t.Fatalf("get cancelled snapshot: %v", err)
	}
	if !reflect.DeepEqual(finalA, cancelledA) {
		t.Fatalf("cancelled snapshot changed: %#v vs %#v", finalA, cancelledA)
	}
}

// Late or duplicate markers for a cancelled or completed ID must not
// regenerate its per-node session state or alter its terminal result.
func TestLateAndDuplicateMarkersDoNotRegenerateState(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)

	// Hold one channel so the first collection stalls after its local cuts
	// propagate; its markers stay queued behind the barrier.
	hold(t, s, 1, 2)
	idA := startSnapshot(t, ctx, s, 1)
	waitAllLocalCuts(t, ctx, s, idA)

	cancelledA, err := s.coord.cancel(ctx, idA)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// injectMarker delivers synthetic markers for the ID to every node, then
	// flushes them through each event loop behind a sentinel retire command.
	injectMarker := func(snapshotID int64) {
		for nodeID := 0; nodeID < nodeCount; nodeID++ {
			for from := 0; from < nodeCount; from++ {
				if from == nodeID {
					continue
				}
				s.nodes[nodeID].events.Put(nodeEvent{ingress: &envelope{marker: &marker{SnapshotID: snapshotID, From: from}}})
			}
		}
		for _, n := range s.nodes {
			if err := n.retire(ctx, -1); err != nil {
				t.Fatalf("sentinel retire: %v", err)
			}
		}
	}
	injectMarker(idA)

	for _, n := range s.nodes {
		if len(n.active) != 0 {
			t.Fatalf("node %d regenerated session state: %#v", n.id+1, n.active)
		}
	}

	// A fresh collection is undisturbed by the cancelled ID's late markers,
	// including the real one still queued behind the barrier.
	idB := startSnapshot(t, ctx, s, 2)
	waitAllLocalCuts(t, ctx, s, idB)
	release(t, s, 1, 2)
	resultB := awaitComplete(t, ctx, s, idB)
	assertTotal(t, resultB)
	assertPerTransferReconciliation(t, resultB)

	// Duplicate markers for a completed ID change nothing either.
	injectMarker(idB)
	for _, n := range s.nodes {
		if len(n.active) != 0 {
			t.Fatalf("node %d regenerated session state: %#v", n.id+1, n.active)
		}
	}
	againB, err := s.getSnapshot(ctx, idB)
	if err != nil {
		t.Fatalf("get completed snapshot: %v", err)
	}
	assertTotal(t, againB)

	finalA, err := s.getSnapshot(ctx, idA)
	if err != nil {
		t.Fatalf("get cancelled snapshot: %v", err)
	}
	if !reflect.DeepEqual(finalA, cancelledA) {
		t.Fatalf("cancelled state changed: %#v vs %#v", finalA, cancelledA)
	}
}

// Default single-snapshot mode still rejects a concurrent start, and cancel
// releases the one slot so a later collection can run.
func TestCancelReleasesSlotInDefaultMode(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx)
	holdAll(t, s)

	firstID := startSnapshot(t, ctx, s, 1)
	if _, err := s.startSnapshot(ctx, 2); err != errSnapshotActive {
		t.Fatalf("second active snapshot error = %v, want %v", err, errSnapshotActive)
	}

	cancelled, err := s.coord.cancel(ctx, firstID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !cancelled.Cancelled || cancelled.Complete || cancelled.ConservationOK {
		t.Fatalf("cancelled snapshot = %#v", cancelled)
	}

	secondID := startSnapshot(t, ctx, s, 2)
	releaseAll(t, s)
	assertTotal(t, awaitComplete(t, ctx, s, secondID))

	again, err := s.getSnapshot(ctx, firstID)
	if err != nil {
		t.Fatalf("get cancelled: %v", err)
	}
	if !again.Cancelled || again.Complete {
		t.Fatalf("cancelled snapshot changed: %#v", again)
	}
}

// A completed snapshot is terminal too: cancel returns it unchanged.
func TestCompletedSnapshotSurvivesCancel(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)

	id := startSnapshot(t, ctx, s, 1)
	completed := awaitComplete(t, ctx, s, id)
	assertTotal(t, completed)

	result, err := s.coord.cancel(ctx, id)
	if err != nil {
		t.Fatalf("cancel completed: %v", err)
	}
	if result.Cancelled || !result.Complete || !result.ConservationOK {
		t.Fatalf("cancel rewrote completed snapshot: %#v", result)
	}
	if result.RecordedTotal != completed.RecordedTotal {
		t.Fatalf("completed total changed: %d vs %d", result.RecordedTotal, completed.RecordedTotal)
	}
}

// The full overlap/cancel scenario over a real HTTP server and client.
func TestHTTPOverlapCancelAndLateMarkers(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	s := newSystem(ctx, true)
	server := httptest.NewServer(newHTTPServer(s))
	defer server.Close()
	client := server.Client()
	base := server.URL

	// Hold 1->2 and park an in-flight transfer behind the barrier.
	doJSON(t, client, http.MethodPut, base+"/test/barriers", barrierRequest{From: 1, To: 2, Held: true}, http.StatusOK, nil)
	var x1 transferResponse
	doJSON(t, client, http.MethodPost, base+"/transfers", transferRequest{ID: "http-x1", From: 1, To: 2, Amount: 30}, http.StatusCreated, &x1)

	// Two overlapping collections are admitted; a third is rejected.
	var snapA snapshotResult
	doJSON(t, client, http.MethodPost, base+"/snapshots", map[string]int{"initiator": 1}, http.StatusAccepted, &snapA)
	if snapA.Complete || snapA.ConservationOK {
		t.Fatalf("snapshot A shows completion behind a held barrier: %#v", snapA)
	}
	var snapB snapshotResult
	doJSON(t, client, http.MethodPost, base+"/snapshots", map[string]int{"initiator": 3}, http.StatusAccepted, &snapB)
	if snapB.ID == snapA.ID {
		t.Fatalf("overlapping snapshots share id %d", snapA.ID)
	}
	doJSON(t, client, http.MethodPost, base+"/snapshots", map[string]int{"initiator": 2}, http.StatusConflict, nil)

	// Both make real partial progress without claiming conservation.
	waitHTTPBalances(t, client, base, snapA.ID)
	waitHTTPBalances(t, client, base, snapB.ID)

	// Cancel A: terminal cancelled state with the accepted partial evidence.
	var cancelledA snapshotResult
	rawCancelled := doJSON(t, client, http.MethodDelete, base+"/snapshots/"+itoa(snapA.ID), nil, http.StatusOK, &cancelledA)
	if !cancelledA.Cancelled || cancelledA.Complete || cancelledA.ConservationOK {
		t.Fatalf("cancelled snapshot A = %#v", cancelledA)
	}
	if len(cancelledA.Balances) != nodeCount || cancelledA.RecordedTotal != 270 {
		t.Fatalf("cancelled evidence = balances %v total %d, want 3 balances totalling 270",
			cancelledA.Balances, cancelledA.RecordedTotal)
	}
	if !hasPendingChannel(&cancelledA, 1, 2) {
		t.Fatalf("cancelled pending = %v, want held channel 1->2", cancelledA.PendingChannels)
	}

	// Repeated cancel and later GETs return the identical terminal state.
	rawAgain := doJSON(t, client, http.MethodDelete, base+"/snapshots/"+itoa(snapA.ID), nil, http.StatusOK, nil)
	if !bytes.Equal(rawAgain, rawCancelled) {
		t.Fatalf("repeated cancel body = %s, want %s", rawAgain, rawCancelled)
	}
	rawGet := doJSON(t, client, http.MethodGet, base+"/snapshots/"+itoa(snapA.ID), nil, http.StatusOK, nil)
	if !bytes.Equal(rawGet, rawCancelled) {
		t.Fatalf("get cancelled body = %s, want %s", rawGet, rawCancelled)
	}

	// The freed slot admits a third collection; transfers keep flowing.
	var snapC snapshotResult
	doJSON(t, client, http.MethodPost, base+"/snapshots", map[string]int{"initiator": 2}, http.StatusAccepted, &snapC)
	waitHTTPBalances(t, client, base, snapC.ID)
	doJSON(t, client, http.MethodPost, base+"/transfers", transferRequest{ID: "http-x2", From: 1, To: 3, Amount: 5}, http.StatusCreated, nil)

	// Releasing the barrier delivers the parked transfer and the cancelled
	// snapshot's late markers; B and C still complete on their own markers.
	doJSON(t, client, http.MethodPut, base+"/test/barriers", barrierRequest{From: 1, To: 2, Held: false}, http.StatusOK, nil)

	var completedB snapshotResult
	awaitHTTPSnapshot(t, client, base, snapB.ID, &completedB)
	assertTotal(t, &completedB)
	assertPerTransferReconciliation(t, &completedB)
	assertBalances(t, &completedB, map[int]int{1: 70, 2: 100, 3: 100})
	assertChannelTrace(t, &completedB, 1, 2, 30, "http-x1")

	var completedC snapshotResult
	awaitHTTPSnapshot(t, client, base, snapC.ID, &completedC)
	assertTotal(t, &completedC)
	assertPerTransferReconciliation(t, &completedC)
	assertChannelTrace(t, &completedC, 1, 2, 30, "http-x1")

	// The late markers did not revive or alter the cancelled terminal state.
	rawFinal := doJSON(t, client, http.MethodGet, base+"/snapshots/"+itoa(snapA.ID), nil, http.StatusOK, nil)
	if !bytes.Equal(rawFinal, rawCancelled) {
		t.Fatalf("cancelled state changed: %s vs %s", rawFinal, rawCancelled)
	}

	// A completed snapshot is not rewritten by a cancel either.
	var deletedB snapshotResult
	doJSON(t, client, http.MethodDelete, base+"/snapshots/"+itoa(snapB.ID), nil, http.StatusOK, &deletedB)
	if deletedB.Cancelled || !deletedB.Complete || !deletedB.ConservationOK {
		t.Fatalf("cancel rewrote completed snapshot: %#v", deletedB)
	}
}

// doJSON performs one real HTTP request and asserts the response status.
func doJSON(t *testing.T, client *http.Client, method, url string, body any, wantStatus int, target any) []byte {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s status = %d body=%s, want %d", method, url, resp.StatusCode, raw, wantStatus)
	}
	if target != nil {
		if err := json.Unmarshal(raw, target); err != nil {
			t.Fatalf("decode response: %v body=%s", err, raw)
		}
	}
	return raw
}

// waitHTTPBalances polls a collecting snapshot until every node's local cut
// is reported, asserting it stays incomplete without conservation claims.
func waitHTTPBalances(t *testing.T, client *http.Client, base string, id int64) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		var result snapshotResult
		doJSON(t, client, http.MethodGet, base+"/snapshots/"+itoa(id), nil, http.StatusAccepted, &result)
		if result.ConservationOK {
			t.Fatalf("incomplete snapshot %d claims conservation", id)
		}
		if len(result.Balances) == nodeCount {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot %d did not record all local cuts", id)
}

// awaitHTTPSnapshot polls until the snapshot completes over real HTTP.
func awaitHTTPSnapshot(t *testing.T, client *http.Client, base string, id int64, target *snapshotResult) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, base+"/snapshots/"+itoa(id), nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("get snapshot %d: %v", id, err)
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read snapshot %d: %v", id, err)
		}
		if resp.StatusCode == http.StatusOK {
			if err := json.Unmarshal(raw, target); err != nil {
				t.Fatalf("decode snapshot %d: %v", id, err)
			}
			if target.Complete {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot %d did not complete", id)
}
