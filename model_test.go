package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
