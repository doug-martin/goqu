package gql

func init() {
	RegisterAdapter("mock", func(ds *Dataset) Adapter {
		return NewDefaultAdapter(ds)
	})
}
