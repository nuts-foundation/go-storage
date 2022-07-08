package stoabs

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTxOptions_RequestsWriteLock(t *testing.T) {
	assert.True(t, TxOptions([]TxOption{WithWriteLock()}).RequestsWriteLock())
	assert.False(t, TxOptions{}.RequestsWriteLock())
}
