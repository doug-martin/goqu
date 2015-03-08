package goqu

type testNoReturnAdapter struct {
	Adapter
}

func (me *testNoReturnAdapter) SupportsReturn() bool {
	return false
}

type testLimitAdapter struct {
	Adapter
}

func (me *testLimitAdapter) SupportsLimitOnDelete() bool {
	return true
}

func (me *testLimitAdapter) SupportsLimitOnUpdate() bool {
	return true
}

type testOrderAdapter struct {
	Adapter
}

func (me *testOrderAdapter) SupportsOrderByOnDelete() bool {
	return true
}

func (me *testOrderAdapter) SupportsOrderByOnUpdate() bool {
	return true
}

func init() {
	RegisterAdapter("mock", func(ds *Dataset) Adapter {
		return NewDefaultAdapter(ds)
	})
	RegisterAdapter("no-return", func(ds *Dataset) Adapter {
		adapter := NewDefaultAdapter(ds)
		return &testNoReturnAdapter{adapter}
	})
	RegisterAdapter("limit", func(ds *Dataset) Adapter {
		adapter := NewDefaultAdapter(ds)
		return &testLimitAdapter{adapter}
	})
	RegisterAdapter("order", func(ds *Dataset) Adapter {
		adapter := NewDefaultAdapter(ds)
		return &testOrderAdapter{adapter}
	})

}
