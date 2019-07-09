package util

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type reflectTest struct {
	suite.Suite
}

func (rt *reflectTest) TestColumnRename() {
	t := rt.T()
	// different key names are used each time to circumvent the caching that happens
	// it seems like a solid assumption that when people use this feature,
	// they would simply set a renaming function once at startup,
	// and not change between requests like this
	lowerAnon := struct {
		FirstLower string
		LastLower  string
	}{}
	lowerColumnMap, lowerErr := GetColumnMap(&lowerAnon)
	assert.NoError(t, lowerErr)

	var lowerKeys []string
	for key := range lowerColumnMap {
		lowerKeys = append(lowerKeys, key)
	}
	assert.Contains(t, lowerKeys, "firstlower")
	assert.Contains(t, lowerKeys, "lastlower")

	// changing rename function
	SetColumnRenameFunction(strings.ToUpper)

	upperAnon := struct {
		FirstUpper string
		LastUpper  string
	}{}
	upperColumnMap, upperErr := GetColumnMap(&upperAnon)
	assert.NoError(t, upperErr)

	var upperKeys []string
	for key := range upperColumnMap {
		upperKeys = append(upperKeys, key)
	}
	assert.Contains(t, upperKeys, "FIRSTUPPER")
	assert.Contains(t, upperKeys, "LASTUPPER")

	SetColumnRenameFunction(defaultColumnRenameFunction)
}

func (rt *reflectTest) TestParallelGetColumnMap() {
	t := rt.T()

	type item struct {
		id   uint
		name string
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		i := item{id: 1, name: "bob"}
		m, err := GetColumnMap(i)
		assert.NoError(t, err)
		assert.NotNil(t, m)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		i := item{id: 2, name: "sally"}
		m, err := GetColumnMap(i)
		assert.NoError(t, err)
		assert.NotNil(t, m)
		wg.Done()
	}()

	wg.Wait()
}

func TestReflectSuite(t *testing.T) {
	suite.Run(t, new(reflectTest))
}
