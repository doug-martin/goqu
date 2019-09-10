package mysql

import (
	"github.com/doug-martin/goqu/v8"
	"github.com/doug-martin/goqu/v8/exp"
)

func DialectOptions() *goqu.SQLDialectOptions {
	opts := goqu.DefaultDialectOptions()

	opts.SupportsReturn = false
	opts.SupportsOrderByOnUpdate = true
	opts.SupportsLimitOnUpdate = true
	opts.SupportsLimitOnDelete = true
	opts.SupportsOrderByOnDelete = true
	opts.SupportsConflictUpdateWhere = false
	opts.SupportsInsertIgnoreSyntax = true
	opts.SupportsConflictTarget = false
	opts.SupportsWithCTE = false
	opts.SupportsWithCTERecursive = false
	opts.SupportsDistinctOn = false
	opts.SupportsWindowFunction = false

	opts.UseFromClauseForMultipleUpdateTables = false

	opts.PlaceHolderRune = '?'
	opts.IncludePlaceholderNum = false
	opts.QuoteRune = '`'
	opts.DefaultValuesFragment = []byte("")
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
		exp.LikeOp:           []byte("LIKE BINARY"),
		exp.NotLikeOp:        []byte("NOT LIKE BINARY"),
		exp.ILikeOp:          []byte("LIKE"),
		exp.NotILikeOp:       []byte("NOT LIKE"),
		exp.RegexpLikeOp:     []byte("REGEXP BINARY"),
		exp.RegexpNotLikeOp:  []byte("NOT REGEXP BINARY"),
		exp.RegexpILikeOp:    []byte("REGEXP"),
		exp.RegexpNotILikeOp: []byte("NOT REGEXP"),
	}
	opts.EscapedRunes = map[rune][]byte{
		'\'': []byte("\\'"),
		'"':  []byte("\\\""),
		'\\': []byte("\\\\"),
		'\n': []byte("\\n"),
		'\r': []byte("\\r"),
		0:    []byte("\\x00"),
		0x1a: []byte("\\x1a"),
	}
	opts.InsertIgnoreClause = []byte("INSERT IGNORE INTO")
	opts.ConflictFragment = []byte("")
	opts.ConflictDoUpdateFragment = []byte(" ON DUPLICATE KEY UPDATE ")
	opts.ConflictDoNothingFragment = []byte("")
	return opts
}

func DialectOptionsV8() *goqu.SQLDialectOptions {
	opts := DialectOptions()
	opts.SupportsWindowFunction = true
	return opts
}

func init() {
	goqu.RegisterDialect("mysql", DialectOptions())
	goqu.RegisterDialect("mysql8", DialectOptionsV8())
}
