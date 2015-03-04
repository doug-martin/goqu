package postgres

import (
	"github.com/doug-martin/gql"
)

const placeholder_rune = '$'

func newDatasetAdapter(ds *gql.Dataset) gql.Adapter {
	ret := gql.NewDefaultAdapter(ds).(*gql.DefaultAdapter)
	ret.PlaceHolderRune = placeholder_rune
	ret.IncludePlaceholderNum = true
	return ret
}

func init() {
	gql.RegisterAdapter("postgres", newDatasetAdapter)
}
