package mssql

import (
	"fmt"
	"strings"
)

// maxIDParts is the number of parts SQL Server allows in an object name:
// server.database.schema.object.
const maxIDParts = 4

// TSQLQuoter implements sqlexp.Quoter
type TSQLQuoter struct {
}

// ID quotes identifiers such as schema, table, or column names.
// This implementation handles multi-part names.
func (TSQLQuoter) ID(name string) string {
	return "[" + strings.Replace(name, "]", "]]", -1) + "]"
}

// splitMultiPartID splits a possibly multi-part name such as "db.schema.table"
// into its individual parts. Dots inside a part delimited with [] or "" are not
// treated as separators.
func splitMultiPartID(name string) []string {
	return splitTopLevel(name, '.')
}

// splitTopLevel splits s on sep, ignoring any separator that appears inside a
// delimited part such as [my.part] or "my.part".
//
// A delimiter only opens a delimited part when it starts that part and has a
// matching closer. A delimiter anywhere else is an ordinary character of an
// undelimited name, so later separators keep splitting and a name such as
// "sch[ema.table" still names the object "table" in the schema "sch[ema".
func splitTopLevel(s string, sep byte) []string {
	parts := make([]string, 0, 4)
	var part strings.Builder
	// blank reports whether the part being read is still empty or holds only
	// leading whitespace, so " [my.part] " is recognised as delimited too.
	blank := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if blank {
			if n := delimitedLen(s[i:]); n > 0 {
				part.WriteString(s[i : i+n])
				i += n - 1
				blank = false
				continue
			}
		}
		if c == sep {
			parts = append(parts, part.String())
			part.Reset()
			blank = true
			continue
		}
		part.WriteByte(c)
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			blank = false
		}
	}
	return append(parts, part.String())
}

// delimitedLen returns the length of the well-formed delimited identifier that
// starts s, such as [my part] or "my part", or 0 when s does not start one.
// A doubled closing delimiter is an escaped literal rather than the end of the
// identifier.
func delimitedLen(s string) int {
	if len(s) < 2 {
		return 0
	}
	var closer byte
	switch s[0] {
	case '[':
		closer = ']'
	case '"':
		closer = '"'
	default:
		return 0
	}
	for i := 1; i < len(s); i++ {
		if s[i] != closer {
			continue
		}
		if i+1 < len(s) && s[i+1] == closer {
			i++
			continue
		}
		return i + 1
	}
	return 0
}

// unquoteID reports whether part is a single well-formed delimited identifier
// and, if so, returns the identifier without its delimiters. A part such as
// `[a] SELECT 1 --[b]` starts and ends with brackets but is not a single
// identifier, so it is rejected. It recognises exactly the parts splitTopLevel
// keeps together, because both decide what a delimited part is with
// delimitedLen.
func unquoteID(part string) (string, bool) {
	// delimitedLen returns 0 for anything that does not start a delimited
	// identifier, which the length comparison alone would accept for an empty
	// part.
	if n := delimitedLen(part); n == 0 || n != len(part) {
		return "", false
	}
	closer := part[len(part)-1]
	body := part[1 : len(part)-1]
	var name strings.Builder
	for i := 0; i < len(body); i++ {
		// delimitedLen already established that every closer in body is
		// doubled, so the second byte of the pair is the literal one.
		if body[i] == closer {
			i++
		}
		name.WriteByte(body[i])
	}
	return name.String(), true
}

// idPartName returns the identifier one part of a multi-part name holds, with
// any delimiters removed. It is empty when the part names nothing, whether it
// was written as "", "  " or "[]".
func idPartName(part string) string {
	part = strings.TrimSpace(part)
	if name, ok := unquoteID(part); ok {
		return name
	}
	return part
}

// quoteIDPart quotes one part of a multi-part name. A part the caller already
// delimited with [] or "" keeps its identifier, everything else is escaped.
// An empty part is preserved so a name such as "db..table" keeps deferring to
// the server's default schema.
func quoteIDPart(part string) string {
	name := idPartName(part)
	if name == "" {
		return ""
	}
	return TSQLQuoter{}.ID(name)
}

// quoteMultiPartID quotes a possibly multi-part object name such as "table",
// "schema.table" or "db.schema.table" so it is safe to embed in query text.
func quoteMultiPartID(name string) string {
	parts := splitMultiPartID(name)
	for i, part := range parts {
		parts[i] = quoteIDPart(part)
	}
	return strings.Join(parts, ".")
}

// quoteObjectName quotes a possibly multi-part object name the same way
// quoteMultiPartID does, but rejects a name that cannot identify an object so
// the caller reports the problem itself instead of leaving the server to fail
// on the query text it produced.
func quoteObjectName(name string) (string, error) {
	parts := splitMultiPartID(name)
	if len(parts) > maxIDParts {
		return "", fmt.Errorf("object name %q has %d parts, at most %d are allowed", name, len(parts), maxIDParts)
	}
	// The last part names the object itself. An empty one means there is no
	// object to name, as in "", "dbo." or "dbo.[]", while an earlier empty
	// part is a qualifier left out on purpose, as in "db..table".
	if idPartName(parts[len(parts)-1]) == "" {
		return "", fmt.Errorf("object name %q does not name an object", name)
	}
	for i, part := range parts {
		parts[i] = quoteIDPart(part)
	}
	return strings.Join(parts, "."), nil
}

// quoteBulkOrder quotes the column names in a BulkOptions.Order entry so they
// are safe to embed in the ORDER hint of an INSERT BULK statement. An entry may
// name a single column or several columns separated by commas. An optional
// trailing ASC or DESC sort direction is preserved. A column name that itself
// ends in "asc" or "desc", or that contains a comma, has to be delimited by the
// caller to be told apart from a sort direction or a separator.
func quoteBulkOrder(entry string) string {
	columns := splitTopLevel(entry, ',')
	for i, column := range columns {
		columns[i] = quoteBulkOrderColumn(column)
	}
	return strings.Join(columns, ",")
}

// quoteBulkOrderColumn quotes a single column of an ORDER hint, keeping any
// trailing sort direction.
func quoteBulkOrderColumn(column string) string {
	name := strings.TrimSpace(column)
	direction := ""
	if i := strings.LastIndexAny(name, " \t\r\n"); i >= 0 {
		switch suffix := strings.ToUpper(strings.TrimSpace(name[i:])); suffix {
		case "ASC", "DESC":
			direction = " " + suffix
			name = name[:i]
		}
	}
	return quoteIDPart(name) + direction
}

// Value quotes database values such as string or []byte types as strings
// that are suitable and safe to embed in SQL text. The returned value
// of a string will include all surrounding quotes.
//
// If a value type is not supported it must panic.
func (TSQLQuoter) Value(v interface{}) string {
	switch v := v.(type) {
	default:
		panic("unsupported value")

	case string:
		return sqlString(v)
	case VarChar:
		return sqlString(string(v))
	case VarCharMax:
		return sqlString(string(v))
	case NVarCharMax:
		return sqlString(string(v))
	}
}

func sqlString(v string) string {
	return "'" + strings.Replace(string(v), "'", "''", -1) + "'"
}
