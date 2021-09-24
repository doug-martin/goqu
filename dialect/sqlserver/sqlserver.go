package sqlserver

import (
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

func DialectOptions() *goqu.SQLDialectOptions {
	opts := goqu.DefaultDialectOptions()

	opts.BooleanDataTypeSupported = false
	opts.UseLiteralIsBools = false

	opts.SupportsReturn = false
	opts.SupportsOrderByOnUpdate = false
	opts.SupportsLimitOnUpdate = false
	opts.SupportsLimitOnDelete = false
	opts.SupportsOrderByOnDelete = true
	opts.SupportsConflictUpdateWhere = false
	opts.SupportsInsertIgnoreSyntax = false
	opts.SupportsConflictTarget = false
	opts.SupportsWithCTE = false
	opts.SupportsWithCTERecursive = false
	opts.SupportsDistinctOn = false
	opts.SupportsWindowFunction = false
	opts.SurroundLimitWithParentheses = true

	opts.PlaceHolderFragment = []byte("@p")
	opts.LimitFragment = []byte(" TOP ")
	opts.IncludePlaceholderNum = true
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
		exp.RegexpLikeOp:     []byte("REGEXP BINARY"),
		exp.RegexpNotLikeOp:  []byte("NOT REGEXP BINARY"),
		exp.RegexpILikeOp:    []byte("REGEXP"),
		exp.RegexpNotILikeOp: []byte("NOT REGEXP"),
	}
	opts.BitwiseOperatorLookup = map[exp.BitwiseOperation][]byte{
		exp.BitwiseInversionOp: []byte("~"),
		exp.BitwiseOrOp:        []byte("|"),
		exp.BitwiseAndOp:       []byte("&"),
		exp.BitwiseXorOp:       []byte("^"),
	}

	opts.FetchFragment = []byte(" FETCH FIRST ")

	opts.SelectSQLOrder = []sqlgen.SQLFragmentType{
		sqlgen.CommonTableSQLFragment,
		sqlgen.SelectWithLimitSQLFragment,
		sqlgen.FromSQLFragment,
		sqlgen.JoinSQLFragment,
		sqlgen.WhereSQLFragment,
		sqlgen.GroupBySQLFragment,
		sqlgen.HavingSQLFragment,
		sqlgen.WindowSQLFragment,
		sqlgen.CompoundsSQLFragment,
		sqlgen.OrderWithOffsetFetchSQLFragment,
		sqlgen.ForSQLFragment,
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

	opts.OfFragment = []byte("")
	opts.ConflictFragment = []byte("")
	opts.ConflictDoUpdateFragment = []byte("")
	opts.ConflictDoNothingFragment = []byte("")

	return opts
}

func init() {
	goqu.RegisterDialect("sqlserver", DialectOptions())
}
