package sqlite3

import (
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
)

func DialectOptions() *goqu.SQLDialectOptions {
	opts := goqu.DefaultDialectOptions()

	opts.SupportsReturn = false
	opts.SupportsOrderByOnUpdate = true
	opts.SupportsLimitOnUpdate = true
	opts.SupportsOrderByOnDelete = true
	opts.SupportsLimitOnDelete = true
	opts.SupportsConflictUpdateWhere = false
	opts.SupportsInsertIgnoreSyntax = true
	opts.SupportsConflictTarget = false
	opts.SupportsMultipleUpdateTables = false
	opts.WrapCompoundsInParens = false
	opts.SupportsDistinctOn = false
	opts.SupportsWindowFunction = false

	opts.PlaceHolderRune = '?'
	opts.IncludePlaceholderNum = false
	opts.QuoteRune = '`'
	opts.DefaultValuesFragment = []byte("")
	opts.True = []byte("1")
	opts.False = []byte("0")
	opts.TimeFormat = "2006-01-02 15:04:05"
	opts.BooleanOperatorLookup = map[exp.BooleanOperation][]byte{
		exp.EqOp:             []byte("="),
		exp.NeqOp:            []byte("!="),
		exp.GtOp:             []byte(">"),
		exp.GteOp:            []byte(">="),
		exp.LtOp:             []byte("<"),
		exp.LteOp:            []byte("<="),
		exp.InOp:             []byte("IN"),
		exp.NotInOp:          []byte("NOT IN"),
		exp.IsOp:             []byte("IS"),
		exp.IsNotOp:          []byte("IS NOT"),
		exp.LikeOp:           []byte("LIKE"),
		exp.NotLikeOp:        []byte("NOT LIKE"),
		exp.ILikeOp:          []byte("LIKE"),
		exp.NotILikeOp:       []byte("NOT LIKE"),
		exp.RegexpLikeOp:     []byte("REGEXP"),
		exp.RegexpNotLikeOp:  []byte("NOT REGEXP"),
		exp.RegexpILikeOp:    []byte("REGEXP"),
		exp.RegexpNotILikeOp: []byte("NOT REGEXP"),
	}
	opts.UseLiteralIsBools = false
	opts.EscapedRunes = map[rune][]byte{
		'\'': []byte("\\'"),
		'"':  []byte("\\\""),
		'\\': []byte("\\\\"),
		'\n': []byte("\\n"),
		'\r': []byte("\\r"),
		0:    []byte("\\x00"),
		0x1a: []byte("\\x1a"),
	}
	opts.InsertIgnoreClause = []byte("INSERT OR IGNORE")
	opts.ConflictFragment = []byte("")
	opts.ConflictDoUpdateFragment = []byte("")
	opts.ConflictDoNothingFragment = []byte("")
	return opts
}

func init() {
	goqu.RegisterDialect("sqlite3", DialectOptions())
}
