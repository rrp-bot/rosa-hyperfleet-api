package dynamodb

import "errors"

// Sentinel errors returned by CRUD methods. Callers use errors.Is() to
// classify failures.
var (
	// ErrNotFound is returned by Get when no item exists for the given key.
	ErrNotFound = errors.New("not found")

	// ErrAlreadyExists is returned by Create when the item already exists.
	ErrAlreadyExists = errors.New("already exists")

	// ErrPreconditionFailed is returned by Replace when the item's version
	// does not match the expected version (optimistic concurrency conflict).
	ErrPreconditionFailed = errors.New("precondition failed")
)

// IsNotFoundError reports whether err is (or wraps) ErrNotFound.
func IsNotFoundError(err error) bool { return errors.Is(err, ErrNotFound) }

// IsAlreadyExistsError reports whether err is (or wraps) ErrAlreadyExists.
func IsAlreadyExistsError(err error) bool { return errors.Is(err, ErrAlreadyExists) }

// IsPreconditionFailedError reports whether err is (or wraps) ErrPreconditionFailed.
func IsPreconditionFailedError(err error) bool { return errors.Is(err, ErrPreconditionFailed) }

func newNotFoundError() error           { return ErrNotFound }
func newAlreadyExistsError() error      { return ErrAlreadyExists }
func newPreconditionFailedError() error { return ErrPreconditionFailed }
