package sb

import (
	"bytes"
	"slices"
	"strconv"
	"sync"
)

const (
	decimalBase = 10
	float64Bits = 64
)

// maxPooledBufferCap is the largest buffer capacity that is kept when a builder is released back to the pool.
// Larger buffers (e.g. from a huge bulk insert) are dropped so a single big statement does not pin memory.
const maxPooledBufferCap = 64 << 10

// Builder that is composed of a bytes.Buffer. It is used internally and by adapters to build SQL statements
type (
	SQLBuilder interface {
		Error() error
		SetError(err error) SQLBuilder
		WriteArg(i ...any) SQLBuilder
		Write(p []byte) SQLBuilder
		WriteStrings(ss ...string) SQLBuilder
		WriteRunes(r ...rune) SQLBuilder
		// WriteInt appends the base 10 representation of i without allocating.
		WriteInt(i int64) SQLBuilder
		// WriteFloat appends f formatted with the 'f' format and the smallest precision that round trips.
		WriteFloat(f float64) SQLBuilder
		// GrowArgs makes room for n more arguments (only useful for prepared statements).
		GrowArgs(n int) SQLBuilder
		// GrowBuffer makes room for n more bytes of SQL.
		GrowBuffer(n int) SQLBuilder
		IsPrepared() bool
		CurrentArgPosition() int
		ToSQL() (sql string, args []any, err error)
	}
	sqlBuilder struct {
		buf *bytes.Buffer
		// True if the sql should not be interpolated
		isPrepared bool
		// Current Number of arguments, used by adapters that need positional placeholders
		currentArgPosition int
		args               []any
		err                error
		// released is true while the builder sits in the pool, it guards against a double release
		released bool
	}
)

var builderPool = sync.Pool{
	New: func() any {
		return newSQLBuilder(false)
	},
}

// NewSQLBuilder returns a builder. Builders come from a pool; call ReleaseSQLBuilder once you are done with one
// (after ToSQL) to make it available for reuse. Not releasing a builder is always safe, it is just garbage collected.
func NewSQLBuilder(isPrepared bool) SQLBuilder {
	b, _ := builderPool.Get().(*sqlBuilder)
	b.released = false
	b.isPrepared = isPrepared
	return b
}

// A fresh and a reset builder must look identical (e.g. nil args) so that they compare equal in tests.
func newSQLBuilder(isPrepared bool) *sqlBuilder {
	return &sqlBuilder{
		buf:                &bytes.Buffer{},
		currentArgPosition: 1,
		isPrepared:         isPrepared,
	}
}

// ReleaseSQLBuilder returns a builder to the pool. The builder must not be used afterwards. The sql string and the
// args returned from ToSQL stay valid: the string is a copy and the args slice is never reused. Releasing a builder
// that is already in the pool is a no-op.
func ReleaseSQLBuilder(b SQLBuilder) {
	sbImpl, ok := b.(*sqlBuilder)
	if !ok || sbImpl.released || sbImpl.buf.Cap() > maxPooledBufferCap {
		return
	}
	sbImpl.reset()
	sbImpl.released = true
	builderPool.Put(sbImpl)
}

func (b *sqlBuilder) reset() {
	b.buf.Reset()
	// Never reuse the args backing array: ToSQL hands it to the caller.
	b.args = nil
	b.err = nil
	b.isPrepared = false
	b.currentArgPosition = 1
}

func (b *sqlBuilder) Error() error {
	return b.err
}

func (b *sqlBuilder) SetError(err error) SQLBuilder {
	if b.err == nil {
		b.err = err
	}
	return b
}

func (b *sqlBuilder) Write(bs []byte) SQLBuilder {
	if b.err == nil {
		b.buf.Write(bs)
	}
	return b
}

func (b *sqlBuilder) WriteStrings(ss ...string) SQLBuilder {
	if b.err == nil {
		for _, s := range ss {
			b.buf.WriteString(s)
		}
	}
	return b
}

func (b *sqlBuilder) WriteRunes(rs ...rune) SQLBuilder {
	if b.err == nil {
		for _, r := range rs {
			b.buf.WriteRune(r)
		}
	}
	return b
}

func (b *sqlBuilder) WriteInt(i int64) SQLBuilder {
	if b.err == nil {
		// AvailableBuffer lets us format straight into the buffer's spare capacity, no scratch space needed.
		b.buf.Write(strconv.AppendInt(b.buf.AvailableBuffer(), i, decimalBase))
	}
	return b
}

func (b *sqlBuilder) WriteFloat(f float64) SQLBuilder {
	if b.err == nil {
		b.buf.Write(strconv.AppendFloat(b.buf.AvailableBuffer(), f, 'f', -1, float64Bits))
	}
	return b
}

func (b *sqlBuilder) GrowArgs(n int) SQLBuilder {
	if b.err == nil && n > 0 {
		b.args = slices.Grow(b.args, n)
	}
	return b
}

func (b *sqlBuilder) GrowBuffer(n int) SQLBuilder {
	if b.err == nil && n > 0 {
		b.buf.Grow(n)
	}
	return b
}

// Returns true if the sql is a prepared statement
func (b *sqlBuilder) IsPrepared() bool {
	return b.isPrepared
}

// Returns the position of the next argument, used by dialects with numbered placeholders
func (b *sqlBuilder) CurrentArgPosition() int {
	return b.currentArgPosition
}

// Adds an argument to the builder, used when IsPrepared is true
func (b *sqlBuilder) WriteArg(i ...any) SQLBuilder {
	if b.err == nil {
		b.currentArgPosition += len(i)
		b.args = append(b.args, i...)
	}
	return b
}

// Returns the sql string, and arguments.
func (b *sqlBuilder) ToSQL() (sql string, args []any, err error) {
	if b.err != nil {
		return sql, args, b.err
	}
	args = b.args
	if args == nil {
		args = []any{}
	}
	return b.buf.String(), args, nil
}
