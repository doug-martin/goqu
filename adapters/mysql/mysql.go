package mysql

import (
    "github.com/doug-martin/gql"
)

var (
    placeholder_rune    = '?'
    quote_rune          = '`'
    singlq_quote        = '\''
    default_values_frag = []byte("")
    mysql_true          = []byte("1")
    mysql_false         = []byte("0")
    time_format         = "2006-01-02 15:04:05"
    operator_lookup     = map[gql.BooleanOperation][]byte{
        gql.EQ_OP:                []byte("="),
        gql.NEQ_OP:               []byte("!="),
        gql.GT_OP:                []byte(">"),
        gql.GTE_OP:               []byte(">="),
        gql.LT_OP:                []byte("<"),
        gql.LTE_OP:               []byte("<="),
        gql.IN_OP:                []byte("IN"),
        gql.NOT_IN_OP:            []byte("NOT IN"),
        gql.IS_OP:                []byte("IS"),
        gql.IS_NOT_OP:            []byte("IS NOT"),
        gql.LIKE_OP:              []byte("LIKE BINARY"),
        gql.NOT_LIKE_OP:          []byte("NOT LIKE BINARY"),
        gql.I_LIKE_OP:            []byte("LIKE"),
        gql.NOT_I_LIKE_OP:        []byte("NOT LIKE"),
        gql.REGEXP_LIKE_OP:       []byte("REGEXP BINARY"),
        gql.REGEXP_NOT_LIKE_OP:   []byte("NOT REGEXP BINARY"),
        gql.REGEXP_I_LIKE_OP:     []byte("REGEXP"),
        gql.REGEXP_NOT_I_LIKE_OP: []byte("NOT REGEXP"),
    }
)

type DatasetAdapter struct {
    *gql.DefaultAdapter
}

func (me *DatasetAdapter) SupportsReturn() bool {
    return false
}

func (me *DatasetAdapter) SupportsLimitOnDelete() bool {
    return true
}

func (me *DatasetAdapter) SupportsLimitOnUpdate() bool {
    return true
}

func (me *DatasetAdapter) SupportsOrderByOnDelete() bool {
    return true
}

func (me *DatasetAdapter) SupportsOrderByOnUpdate() bool {
    return true
}

func (me *DatasetAdapter) LiteralString(buf *gql.SqlBuilder, s string) error {
    if buf.IsPrepared {
        return me.PlaceHolderSql(buf, s)
    }
    buf.WriteRune(singlq_quote)
    for _, char := range s {
        if char == '\'' { // single quote: ' -> \'
            buf.WriteString("\\'")
        } else if char == '"' { // double quote: " -> \"
            buf.WriteString("\\\"")
        } else if char == '\\' { // slash: \ -> "\\"
            buf.WriteString("\\\\")
        } else if char == '\n' { // control: newline: \n -> "\n"
            buf.WriteString("\\n")
        } else if char == '\r' { // control: return: \r -> "\r"
            buf.WriteString("\\r")
        } else if char == 0 { // control: NUL: 0 -> "\x00"
            buf.WriteString("\\x00")
        } else if char == 0x1a { // control: \x1a -> "\x1a"
            buf.WriteString("\\x1a")
        } else {
            buf.WriteRune(char)
        }
    }
    buf.WriteRune(singlq_quote)
    return nil
}

func newDatasetAdapter(ds *gql.Dataset) gql.Adapter {
    def := gql.NewDefaultAdapter(ds).(*gql.DefaultAdapter)
    def.PlaceHolderRune = placeholder_rune
    def.IncludePlaceholderNum = false
    def.QuoteRune = quote_rune
    def.DefaultValuesFragment = default_values_frag
    def.True = mysql_true
    def.False = mysql_false
    def.TimeFormat = time_format
    def.BooleanOperatorLookup = operator_lookup
    return &DatasetAdapter{def}
}


func init() {
	gql.RegisterAdapter("mysql", newDatasetAdapter)
}
