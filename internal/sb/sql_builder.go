package sb

import (
	"bytes"
	"sync"
)

// Builder that is composed of a bytes.Buffer. It is used internally and by adapters to build SQL statements
type (
	SQLBuilder interface {
		Error() error
		SetError(err error) SQLBuilder
		WriteArg(i ...interface{}) SQLBuilder
		Write(p []byte) SQLBuilder
		WriteStrings(ss ...string) SQLBuilder
		WriteRunes(r ...rune) SQLBuilder
		GrowArgs(n int) SQLBuilder
		GrowBuffer(n int) SQLBuilder
		IsPrepared() bool
		CurrentArgPosition() int
		ToSQL() (sql string, args []interface{}, err error)
	}
	sqlBuilder struct {
		buf *bytes.Buffer
		// True if the sql should not be interpolated
		isPrepared bool
		// Current Number of arguments, used by adapters that need positional placeholders
		currentArgPosition int
		args               []any
		err                error
	}
)

func NewSQLBuilder(isPrepared bool) SQLBuilder {
	sb := builderPool.Get().(*sqlBuilder)
	sb.isPrepared = isPrepared
	return sb
}

func newSQLBuilder(isPrepared bool) *sqlBuilder {
	return &sqlBuilder{
		buf:                &bytes.Buffer{},
		args:               make([]any, 0, 4),
		currentArgPosition: 1,
		isPrepared:         isPrepared,
	}
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
		switch len(ss) {
		case 0:
			return b
		case 1:
			b.buf.WriteString(ss[0])
		default:
			totalSize := 0
			for _, s := range ss {
				totalSize += len(s)
			}
			b.buf.Grow(totalSize)
			for _, s := range ss {
				b.buf.WriteString(s)
			}
		}
	}
	return b
}

func (b *sqlBuilder) WriteRunes(rs ...rune) SQLBuilder {
	if b.err == nil {
		switch len(rs) {
		case 0:
			return b
		case 1:
			b.buf.WriteRune(rs[0])
		case 2:
			b.buf.Grow(2)
			b.buf.WriteRune(rs[0])
			b.buf.WriteRune(rs[1])
		default:
			b.buf.WriteString(string(rs))
		}
	}
	return b
}

func (b *sqlBuilder) GrowArgs(n int) SQLBuilder {
	if b.err != nil || n <= 0 {
		return b
	}
	currentLen := len(b.args)
	needed := currentLen + n
	if cap(b.args) >= needed {
		return b
	}
	newCap := cap(b.args)
	if newCap == 0 {
		newCap = 1
	}
	for newCap < needed {
		newCap *= 2
	}
	na := make([]interface{}, currentLen, newCap)
	copy(na, b.args)
	b.args = na
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

// Returns true if the sql is a prepared statement
func (b *sqlBuilder) CurrentArgPosition() int {
	return b.currentArgPosition
}

// Adds an argument to the builder, used when IsPrepared is false
func (b *sqlBuilder) WriteArg(i ...interface{}) SQLBuilder {
	if b.err == nil {
		b.currentArgPosition += len(i)
		b.args = append(b.args, i...)
	}
	return b
}

// Returns the sql string, and arguments.
func (b *sqlBuilder) ToSQL() (sql string, args []interface{}, err error) {
	if b.err != nil {
		return sql, args, b.err
	}
	return b.buf.String(), b.args, nil
}

var builderPool = sync.Pool{
	New: func() interface{} {
		return newSQLBuilder(false)
	},
}

func (b *sqlBuilder) reset() {
	b.buf.Reset()
	b.args = make([]any, 0, 4)
	b.err = nil
	b.isPrepared = false
	b.currentArgPosition = 1
}

func ReleaseSQLBuilder(b SQLBuilder) {
	if sbImpl, ok := b.(*sqlBuilder); ok {
		sbImpl.reset()
		builderPool.Put(sbImpl)
	}
}
