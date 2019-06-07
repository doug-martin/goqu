package goqu

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type adapterTest struct {
	suite.Suite
}

func (me *adapterTest) TestHasAdapter() {
	t := me.T()
	assert.False(t, HasAdapter("test"))
	RegisterAdapter("test", func(ds *Dataset) Adapter {
		return NewDefaultAdapter(ds)
	})
	assert.True(t, HasAdapter("test"))
	removeAdapter("test")
}

func (me *adapterTest) TestRegisterAdapter() {
	t := me.T()
	RegisterAdapter("test", func(ds *Dataset) Adapter {
		return NewDefaultAdapter(ds)
	})
	assert.True(t, HasAdapter("test"))
	removeAdapter("test")
}

func (me *adapterTest) TestNewAdapter() {
	t := me.T()
	RegisterAdapter("test", func(ds *Dataset) Adapter {
		return NewDefaultAdapter(ds)
	})
	assert.True(t, HasAdapter("test"))
	adapter := NewAdapter("test", From("test"))
	assert.NotNil(t, adapter)
	removeAdapter("test")

	adapter = NewAdapter("test", From("test"))
	assert.NotNil(t, adapter)
}

func TestAdapterSuite(t *testing.T) {
	suite.Run(t, new(adapterTest))
}
