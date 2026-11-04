package sb //nolint:testpackage // exercises unexported reset/pool state

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// Ensures that after calling ToSQL(), the returned args slice is not mutated
// by subsequent calls to reset() and further writes on the same builder.
func TestSQLBuilder_ResetDoesNotMutateReturnedArgs(t *testing.T) {
	// Use the unexported constructor to deterministically operate on a single builder instance.
	sb := newSQLBuilder(false)

	sb.WriteArg("first", 2)
	_, argsBefore, err := sb.ToSQL()
	if err != nil {
		t.Fatalf("unexpected error from ToSQL: %v", err)
	}
	if len(argsBefore) != 2 {
		t.Fatalf("expected 2 args before reset, got %d", len(argsBefore))
	}
	argsSnapshot := append([]any(nil), argsBefore...)

	// If reset reused the same backing array for args, the following WriteArg calls would overwrite argsBefore.
	sb.reset()
	sb.WriteArg("new1", "new2")

	if !reflect.DeepEqual(argsBefore, argsSnapshot) {
		t.Fatalf("args returned by first ToSQL were mutated after reset+reuse; before=%v after=%v", argsSnapshot, argsBefore)
	}
}

// The sql string returned by ToSQL must stay valid after the builder is released and reused.
func TestSQLBuilder_ReleaseDoesNotMutateReturnedSQL(t *testing.T) {
	b := NewSQLBuilder(false)
	b.WriteStrings("SELECT 1")
	sql, _, err := b.ToSQL()
	if err != nil {
		t.Fatal(err)
	}
	ReleaseSQLBuilder(b)

	// grab builders until we (very likely) get the released one back, then overwrite its buffer
	for i := 0; i < 8; i++ {
		other := NewSQLBuilder(false)
		other.WriteStrings("XXXXXXXX")
		ReleaseSQLBuilder(other)
	}
	if sql != "SELECT 1" {
		t.Fatalf("sql returned by ToSQL changed after release; got %q", sql)
	}
}

// Ensures that releasing a builder resets its state, so a reused builder behaves like a new one.
func TestReleaseSQLBuilderResetsState(t *testing.T) {
	b := NewSQLBuilder(true)
	b.WriteStrings("SELECT ")
	b.WriteRunes('1')
	b.WriteArg(123)
	b.SetError(errors.New("assert"))

	sbImpl, ok := b.(*sqlBuilder)
	if !ok {
		t.Fatalf("expected *sqlBuilder implementation")
	}

	ReleaseSQLBuilder(b)

	if got := sbImpl.buf.String(); got != "" {
		t.Fatalf("buffer not reset; got %q", got)
	}
	if sbImpl.args != nil {
		t.Fatalf("args not reset; got %v", sbImpl.args)
	}
	if sbImpl.err != nil {
		t.Fatalf("error not reset; got %v", sbImpl.err)
	}
	if sbImpl.isPrepared {
		t.Fatalf("isPrepared not reset; got true, want false")
	}
	if sbImpl.currentArgPosition != 1 {
		t.Fatalf("currentArgPosition not reset; got %d, want 1", sbImpl.currentArgPosition)
	}
}

// Huge buffers are dropped instead of being pinned by the pool.
func TestReleaseSQLBuilderDropsOversizedBuffers(t *testing.T) {
	big := newSQLBuilder(false)
	big.GrowBuffer(maxPooledBufferCap * 2)
	// Put a marker in the pool's way: if big were pooled, Get could hand it back with its huge capacity.
	ReleaseSQLBuilder(big)
	for i := 0; i < 64; i++ {
		b, _ := NewSQLBuilder(false).(*sqlBuilder)
		if b == big {
			t.Fatal("oversized builder was returned to the pool")
		}
	}
}

// A new builder, a reset builder and a pooled builder must be indistinguishable (matters for equality in tests).
func TestFreshAndResetBuildersAreEqual(t *testing.T) {
	used := newSQLBuilder(false)
	used.WriteStrings("abc")
	used.WriteArg(1)
	used.reset()
	fresh := newSQLBuilder(false)
	if used.currentArgPosition != fresh.currentArgPosition || used.isPrepared != fresh.isPrepared ||
		!errors.Is(used.err, fresh.err) || !reflect.DeepEqual(used.args, fresh.args) {
		t.Fatalf("reset builder differs from a fresh one: %+v vs %+v", used, fresh)
	}
}

func TestSQLBuilder_ToSQLReturnsNonNilArgs(t *testing.T) {
	_, args, err := newSQLBuilder(true).ToSQL()
	if err != nil {
		t.Fatal(err)
	}
	if args == nil || len(args) != 0 {
		t.Fatalf("expected an empty, non-nil args slice; got %#v", args)
	}
}

func TestSQLBuilder_WriteIntAndFloat(t *testing.T) {
	b := newSQLBuilder(false)
	b.WriteInt(0).WriteStrings(",").
		WriteInt(-42).WriteStrings(",").
		WriteInt(math.MaxInt64).WriteStrings(",").
		WriteInt(math.MinInt64).WriteStrings(",").
		WriteFloat(1.5).WriteStrings(",").
		WriteFloat(14000000.5).WriteStrings(",").
		WriteFloat(1e21).WriteStrings(",").
		WriteFloat(0.000001)
	sql, _, err := b.ToSQL()
	if err != nil {
		t.Fatal(err)
	}
	expected := "0,-42,9223372036854775807,-9223372036854775808,1.5,14000000.5,1000000000000000000000,0.000001"
	if sql != expected {
		t.Fatalf("got %q, want %q", sql, expected)
	}

	// once an error is set nothing is written
	errB := newSQLBuilder(false).SetError(errors.New("boom"))
	errB.WriteInt(1).WriteFloat(2)
	if got := errB.(*sqlBuilder).buf.String(); got != "" {
		t.Fatalf("builder with an error must not write; got %q", got)
	}
}

func TestSQLBuilder_Grow(t *testing.T) {
	b := newSQLBuilder(true)
	b.WriteArg("a")
	b.GrowArgs(100)
	if cap(b.args)-len(b.args) < 100 {
		t.Fatalf("GrowArgs did not make room for 100 args: len=%d cap=%d", len(b.args), cap(b.args))
	}
	if len(b.args) != 1 || b.args[0] != "a" {
		t.Fatalf("GrowArgs must keep existing args; got %v", b.args)
	}
	b.GrowArgs(0)
	b.GrowArgs(-1)
	b.GrowBuffer(0)
	b.GrowBuffer(-5)
	b.GrowBuffer(1000)
	if b.buf.Cap() < 1000 {
		t.Fatalf("GrowBuffer did not grow the buffer; cap=%d", b.buf.Cap())
	}

	errB := newSQLBuilder(true).SetError(errors.New("boom")).(*sqlBuilder)
	errB.GrowArgs(10)
	errB.GrowBuffer(10)
	if cap(errB.args) != 0 {
		t.Fatalf("GrowArgs must be a no-op on a builder with an error; cap=%d", cap(errB.args))
	}
}

// Builders are pooled, so hammer the pool from many goroutines (run with -race) and make sure no state leaks
// between statements.
func TestSQLBuilder_PoolIsSafeForConcurrentUse(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			marker := strings.Repeat(string("abcdefgh"[g%8]), 5)
			for i := 0; i < 2000; i++ {
				b := NewSQLBuilder(i%2 == 0)
				if b.CurrentArgPosition() != 1 {
					t.Errorf("goroutine %d: reused builder has arg position %d", g, b.CurrentArgPosition())
					return
				}
				b.WriteStrings(marker).WriteInt(int64(i)).WriteArg(g, i)
				sql, args, err := b.ToSQL()
				if err != nil || sql != marker+itoa(i) || len(args) != 2 || args[0] != g || args[1] != i {
					t.Errorf("goroutine %d: leaked state; sql=%q args=%v err=%v", g, sql, args, err)
					return
				}
				ReleaseSQLBuilder(b)
			}
		}(g)
	}
	wg.Wait()
}

func itoa(i int) string {
	b := newSQLBuilder(false)
	b.WriteInt(int64(i))
	s, _, _ := b.ToSQL()
	return s
}

func TestReleaseSQLBuilder_doubleReleaseIsIgnored(t *testing.T) {
	b := NewSQLBuilder(false)
	ReleaseSQLBuilder(b)
	ReleaseSQLBuilder(b) // must not put the same builder in the pool twice
	first, second := NewSQLBuilder(false), NewSQLBuilder(false)
	if first == second {
		t.Fatal("the same builder was handed out twice")
	}
}
