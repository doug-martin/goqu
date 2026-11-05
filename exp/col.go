package exp

import (
	"fmt"
	"reflect"

	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/util"
)

type columnList struct {
	columns []Expression
}

// NewColumnListExpression creates a column list. A struct is expanded to all of its columns (INSERT, GROUP BY,
// RETURNING, ...), including the ones tagged skipselect.
func NewColumnListExpression(vals ...interface{}) ColumnListExpression {
	cols, _ := newColumnList(false, vals)
	return columnList{columns: cols}
}

// NewSelectColumnListExpression creates the column list of a SELECT. It is the same as NewColumnListExpression except
// the columns of a struct tagged skipselect are left out. It returns an error if that leaves nothing to select
// (an empty list would be rendered as *, selecting the columns that should be skipped).
func NewSelectColumnListExpression(vals ...interface{}) (ColumnListExpression, error) {
	cols, skippedAll := newColumnList(true, vals)
	if len(cols) == 0 && skippedAll {
		return nil, errors.New("all of the columns of the struct are tagged skipselect, there is nothing to select")
	}
	return columnList{columns: cols}, nil
}

// newColumnList builds the columns, skippedAll is true when a struct had columns but all of them were skipped.
func newColumnList(forSelect bool, vals []interface{}) (cols []Expression, skippedAll bool) {
	cols = []Expression{}
	for _, val := range vals {
		switch t := val.(type) {
		case nil: // do nothing
		case string:
			cols = append(cols, ParseIdentifier(t))
		case ColumnListExpression:
			cols = append(cols, t.Columns()...)
		case Expression:
			cols = append(cols, t)
		default:
			_, valKind := util.GetTypeInfo(val, reflect.Indirect(reflect.ValueOf(val)))

			if valKind == reflect.Struct {
				cm, err := util.GetColumnMap(val)
				if err != nil {
					panic(err.Error())
				}
				structCols := cm.Cols()
				added := 0
				for _, col := range structCols {
					if forSelect && cm[col].SkipSelect {
						continue
					}
					added++
					i := ParseIdentifier(col)
					var sc Expression = i
					if i.IsQualified() {
						sc = i.As(NewIdentifierExpression("", "", col))
					}
					cols = append(cols, sc)
				}
				if len(structCols) > 0 && added == 0 {
					skippedAll = true
				}
			} else {
				panic(fmt.Sprintf("Cannot create expression from  %+v", val))
			}
		}
	}
	return cols, skippedAll
}

func NewOrderedColumnList(vals ...OrderedExpression) ColumnListExpression {
	exps := make([]interface{}, 0, len(vals))
	for _, col := range vals {
		exps = append(exps, col.Expression())
	}
	return NewColumnListExpression(exps...)
}

func (cl columnList) Clone() Expression {
	newExps := make([]Expression, 0, len(cl.columns))
	for _, exp := range cl.columns {
		newExps = append(newExps, exp.Clone())
	}
	return columnList{columns: newExps}
}

func (cl columnList) Expression() Expression {
	return cl
}

func (cl columnList) IsEmpty() bool {
	return len(cl.columns) == 0
}

func (cl columnList) Columns() []Expression {
	return cl.columns
}

func (cl columnList) Append(cols ...Expression) ColumnListExpression {
	ret := columnList{}
	exps := ret.columns
	exps = append(exps, cl.columns...)
	exps = append(exps, cols...)
	ret.columns = exps
	return ret
}
