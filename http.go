package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
)

type server struct {
	system *system
	mux    *http.ServeMux
}

func newHTTPServer(system *system) http.Handler {
	s := &server{system: system, mux: http.NewServeMux()}
	s.routes()
	return s.mux
}

func (s *server) routes() {
	s.mux.HandleFunc("GET /health", s.health)
	s.mux.HandleFunc("POST /transfers", s.createTransfer)
	s.mux.HandleFunc("POST /snapshots", s.createSnapshot)
	s.mux.HandleFunc("GET /snapshots/{id}", s.getSnapshot)
	s.mux.HandleFunc("DELETE /snapshots/{id}", s.cancelSnapshot)
	s.mux.HandleFunc("PUT /test/barriers", s.setBarrier)
	s.mux.HandleFunc("GET /test/barriers", s.listBarriers)
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) createTransfer(w http.ResponseWriter, r *http.Request) {
	var req transferRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	result, err := s.system.submitTransfer(r.Context(), req)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (s *server) createSnapshot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Initiator int `json:"initiator"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	id, err := s.system.startSnapshot(r.Context(), req.Initiator)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	result, err := s.system.getSnapshot(r.Context(), id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	status := http.StatusAccepted
	if result.Complete {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

func (s *server) getSnapshot(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("snapshot id must be a positive integer"))
		return
	}

	result, err := s.system.getSnapshot(r.Context(), id)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	status := http.StatusOK
	if !result.Complete && !result.Cancelled {
		// Explicitly return partial, real state; never pretend completion.
		status = http.StatusAccepted
	}
	writeJSON(w, status, result)
}

func (s *server) setBarrier(w http.ResponseWriter, r *http.Request) {
	var req barrierRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	view, err := s.system.setBarrier(req)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *server) listBarriers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"channels": s.system.barriers()})
}

func decodeJSON(r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid JSON request body")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid JSON request body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errInsufficientFunds):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, errDuplicateTransfer), errors.Is(err, errSnapshotActive):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, errSnapshotNotFound):
		writeError(w, http.StatusNotFound, err)
	default:
		writeError(w, http.StatusBadRequest, err)
	}
}

func (s *server) cancelSnapshot(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid snapshot id"))
		return
	}
	result, err := s.system.coord.cancel(r.Context(), id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
