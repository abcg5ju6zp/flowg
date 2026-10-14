package interfaces

import "errors"

// ErrStreamRetiring is returned by write and configuration operations while a
// stream is fenced by an in-progress purge (its "retirement generation").
// In-flight writes admitted before the fence are allowed to finish; every
// operation reaching the storage afterwards fails with this definite error
// until the purge completes and the stream is recreated under a new
// generation.
var ErrStreamRetiring = errors.New("stream is being purged")
