package sb

import (
	"reflect"
	"testing"
)

// Ensures that after calling ToSQL(), the returned args slice is not mutated
// by subsequent calls to reset() and further writes on the same builder.
func TestSQLBuilder_ResetDoesNotMutateReturnedArgs(t *testing.T) {
	// Use the unexported constructor to deterministically operate on a single builder instance.
	sb := newSQLBuilder(false)

	// Populate with initial args and capture the returned args slice from ToSQL.
	sb.WriteArg("first", 2)
	_, argsBefore, err := sb.ToSQL()
	if err != nil {
		t.Fatalf("unexpected error from ToSQL: %v", err)
	}
	if len(argsBefore) != 2 {
		t.Fatalf("expected 2 args before reset, got %d", len(argsBefore))
	}

	// Keep a deep copy for comparison after reset and reuse.
	argsSnapshot := append([]interface{}(nil), argsBefore...)

	// Reset and reuse the builder; if reset reuses the same backing array for args,
	// the following WriteArg calls would overwrite argsBefore's elements.
	sb.reset()
	sb.WriteArg("new1", "new2")

	// Verify that the previously returned args slice remains unchanged.
	if !reflect.DeepEqual(argsBefore, argsSnapshot) {
		t.Fatalf("args returned by first ToSQL were mutated after reset+reuse; before=%v after=%v", argsSnapshot, argsBefore)
	}
}

// Ensures that releasing a builder resets its fields, including creating a fresh args slice
// (cap reset to 4 as per implementation), clearing buffer, error, prepared flag, and arg position.
func TestReleaseSQLBuilderResetsState(t *testing.T) {
	b := NewSQLBuilder(true)
	// Exercise fields so we can verify they get reset.
	b.WriteStrings("SELECT ")
	b.WriteRunes('1')
	b.WriteArg(123)
	// Set an error to make sure it's cleared by reset via Release.
	b.SetError(assertErr{})

	// Keep a reference to the underlying implementation for inspection after release.
	sbImpl, ok := b.(*sqlBuilder)
	if !ok {
		t.Fatalf("expected *sqlBuilder implementation")
	}

	ReleaseSQLBuilder(b)

	// Buffer should be cleared.
	if got := sbImpl.buf.String(); got != "" {
		t.Fatalf("buffer not reset; got %q", got)
	}
	// Args should be a fresh slice with len 0 and cap 4.
	if ln := len(sbImpl.args); ln != 0 {
		t.Fatalf("args length not reset; got %d, want 0", ln)
	}
	if cap(sbImpl.args) != 4 {
		t.Fatalf("args capacity not reset; got %d, want 4", cap(sbImpl.args))
	}
	// Error cleared
	if sbImpl.err != nil {
		t.Fatalf("error not reset; got %v", sbImpl.err)
	}
	// Prepared flag reset
	if sbImpl.isPrepared {
		t.Fatalf("isPrepared not reset; got true, want false")
	}
	// Arg position reset
	if sbImpl.currentArgPosition != 1 {
		t.Fatalf("currentArgPosition not reset; got %d, want 1", sbImpl.currentArgPosition)
	}
}

// assertErr is a sentinel error type for testing SetError/Reset clearing.
type assertErr struct{}

func (assertErr) Error() string { return "assert" }


