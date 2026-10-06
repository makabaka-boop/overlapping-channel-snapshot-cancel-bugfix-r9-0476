package main

import "errors"

var (
	errInsufficientFunds = errors.New("insufficient funds")
	errInvalidNode       = errors.New("node must be 1, 2, or 3")
	errInvalidAmount     = errors.New("amount must be a positive integer")
	errDuplicateTransfer = errors.New("correlation id has already been used")
	errSnapshotActive    = errors.New("another snapshot is still active")
	errSnapshotNotFound  = errors.New("snapshot not found")
)
