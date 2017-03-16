package postgres

import (
	"gopkg.in/doug-martin/goqu.v4"
)

const placeholder_rune = '$'

func newDatasetAdapter(ds *goqu.Dataset) goqu.Adapter {
	ret := goqu.NewDefaultAdapter(ds).(*goqu.DefaultAdapter)
	ret.PlaceHolderRune = placeholder_rune
	ret.IncludePlaceholderNum = true
	return ret
}

func init() {
	goqu.RegisterAdapter("postgres", newDatasetAdapter)
}
