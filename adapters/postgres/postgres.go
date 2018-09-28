package postgres

import (
	"github.com/doug-martin/goqu/v6"
)

const placeholder_rune = '$'

type DatasetAdapter struct {
	*goqu.DefaultAdapter
}

//Generates SQL for a slice of values (e.g. []int64{1,2,3,4} -> (1,2,3,4)
func (me *DatasetAdapter) SliceValueSql(buf *goqu.SqlBuilder, slice reflect.Value) error {
	buf.WriteString("ARRAY[")

	for i, l := 0, slice.Len(); i < l; i++ {
		if err := me.Literal(buf, slice.Index(i).Interface()); err != nil {
			return err
		}
		if i < l - 1 {
			buf.WriteRune(comma_rune)
			buf.WriteRune(space_rune)
		}
	}
	buf.WriteRune(']')
	return nil
}

func newDatasetAdapter(ds *goqu.Dataset) goqu.Adapter {
	ret := goqu.NewDefaultAdapter(ds).(*goqu.DefaultAdapter)
	ret.PlaceHolderRune = placeholder_rune
	ret.IncludePlaceholderNum = true
	return &DatasetAdapter{ret}
}


func init() {
	goqu.RegisterAdapter("postgres", newDatasetAdapter)
}
