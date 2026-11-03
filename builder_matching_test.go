package goqu_test

import (
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/stretchr/testify/mock"
)

// builderMatching matches any builder with the same prepared flag as expected.
//
// SQL builders are pooled, so a builder that was used before can never be deeply equal to a freshly created one
// (a reused bytes.Buffer keeps its backing slice). Comparing them by value would make mock based tests depend on the
// state of the pool, so match on what the dataset is supposed to control instead.
func builderMatching(expected sb.SQLBuilder) any {
	return mock.MatchedBy(func(actual sb.SQLBuilder) bool {
		return actual != nil && actual.IsPrepared() == expected.IsPrepared() && actual.Error() == nil
	})
}
