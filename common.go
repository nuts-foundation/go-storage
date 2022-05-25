package stoabs

import "errors"

// ErrCommitFailed is returned when the commit of transaction fails.
var ErrCommitFailed = errors.New("unable to commit transaction")

type Store interface {
	// Close releases all resources associated with the store. It is safe to call multiple (subsequent) times.
	Close() error
}

// TxOption holds options for store transactions.
type TxOption interface{}

type TxOptions []TxOption

func (opts TxOptions) InvokeAfterRollback() {
	for _, opt := range opts {
		if ar, ok := opt.(*AfterRollbackOpt); ok {
			ar.Func()
		}
	}
}

func (opts TxOptions) InvokeAfterCommit() {
	for _, opt := range opts {
		if ar, ok := opt.(*AfterCommitOpt); ok {
			ar.Func()
		}
	}
}

type AfterCommitOpt struct {
	Func func()
}

// AfterCommit specifies a function that will be called after a transaction is successfully committed.
// There can be multiple AfterCommit functions, which will be called in order.
func AfterCommit(fn func()) TxOption {
	return &AfterCommitOpt{Func: fn}
}

type AfterRollbackOpt struct {
	Func func()
}

// AfterRollback specifies a function that will be called after a transaction is successfully rolled back.
// There can be multiple AfterRollback functions, which will be called in order.
func AfterRollback(fn func()) TxOption {
	return &AfterRollbackOpt{Func: fn}
}

// WriteTx is used to write to a KVStore.
type WriteTx interface {
	ReadTx
	// GetShelfWriter returns the specified shelf for writing. If it doesn't exist, it will be created.
	GetShelfWriter(shelfName string) (Writer, error)
}

// ReadTx is used to read from a KVStore.
type ReadTx interface {
	// GetShelfReader returns the specified shelf for reading. If it doesn't exist, nil is returned.
	GetShelfReader(shelfName string) (Reader, error)
}