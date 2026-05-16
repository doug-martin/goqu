package exp

type (
	doNothingConflict struct{}
	// ConflictTarget holds the conflict target column(s) for an ON CONFLICT clause,
	// with an optional index predicate (WHERE clause on the target itself).
	ConflictTarget struct {
		target      string
		whereClause ExpressionList
	}
	// ConflictUpdate is the struct that represents the UPDATE fragment of an
	// INSERT ... ON CONFLICT/ON DUPLICATE KEY DO UPDATE statement
	conflictUpdate struct {
		target         string
		conflictTarget *ConflictTarget
		update         interface{}
		whereClause    ExpressionList
	}
)

// NewConflictTarget creates a ConflictTarget for use with DoUpdate.
// Call .Where(...) on it to add an index predicate to the conflict target.
func NewConflictTarget(target string) *ConflictTarget {
	return &ConflictTarget{target: target}
}

func (ct *ConflictTarget) Column() string {
	return ct.target
}

func (ct *ConflictTarget) WhereClause() ExpressionList {
	return ct.whereClause
}

func (ct *ConflictTarget) Where(expressions ...Expression) *ConflictTarget {
	if ct.whereClause == nil {
		ct.whereClause = NewExpressionList(AndType, expressions...)
	} else {
		ct.whereClause = ct.whereClause.Append(expressions...)
	}
	return ct
}

// Creates a conflict struct to be passed to InsertConflict to ignore constraint errors
//
//	InsertConflict(DoNothing(),...) -> INSERT INTO ... ON CONFLICT DO NOTHING
func NewDoNothingConflictExpression() ConflictExpression {
	return &doNothingConflict{}
}

func (c doNothingConflict) Expression() Expression {
	return c
}

func (c doNothingConflict) Clone() Expression {
	return c
}

func (c doNothingConflict) Action() ConflictAction {
	return DoNothingConflictAction
}

// Creates a ConflictUpdate struct to be passed to InsertConflict
// Represents a ON CONFLICT DO UPDATE portion of an INSERT statement (ON DUPLICATE KEY UPDATE for mysql)
//
//	InsertConflict(DoUpdate("target_column", update),...) ->
//		INSERT INTO ... ON CONFLICT DO UPDATE SET a=b
//	InsertConflict(DoUpdate("target_column", update).Where(Ex{"a": 1},...) ->
//		INSERT INTO ... ON CONFLICT DO UPDATE SET a=b WHERE a=1
func NewDoUpdateConflictExpression(target string, update interface{}) ConflictUpdateExpression {
	return &conflictUpdate{target: target, update: update}
}

// NewDoUpdateConflictExpressionWithTarget creates a ConflictUpdateExpression using a ConflictTarget,
// which supports an optional index predicate via .Where().
func NewDoUpdateConflictExpressionWithTarget(ct *ConflictTarget, update interface{}) ConflictUpdateExpression {
	return &conflictUpdate{conflictTarget: ct, update: update}
}

func (c conflictUpdate) Expression() Expression {
	return c
}

func (c conflictUpdate) Clone() Expression {
	return &conflictUpdate{
		target:         c.target,
		conflictTarget: c.conflictTarget,
		update:         c.update,
		whereClause:    c.whereClause.Clone().(ExpressionList),
	}
}

func (c conflictUpdate) Action() ConflictAction {
	return DoUpdateConflictAction
}

// Returns the target conflict column. Only necessary for Postgres.
// Will return an error for mysql/sqlite. Will also return an error if missing from a postgres ConflictUpdate.
func (c conflictUpdate) TargetColumn() string {
	if c.conflictTarget != nil {
		return c.conflictTarget.Column()
	}
	return c.target
}

// ConflictTargetClause returns the ConflictTarget when DoUpdate was called with one (non-nil means
// an index predicate may be present). Returns nil for plain string targets.
func (c conflictUpdate) ConflictTargetClause() *ConflictTarget {
	return c.conflictTarget
}

// Returns the Updates which represent the ON CONFLICT DO UPDATE portion of an insert statement. If nil,
// there are no updates.
func (c conflictUpdate) Update() interface{} {
	return c.update
}

// Append to the existing Where clause for an ON CONFLICT DO UPDATE ... WHERE ...
//
//	InsertConflict(DoNothing(),...) -> INSERT INTO ... ON CONFLICT DO NOTHING
func (c *conflictUpdate) Where(expressions ...Expression) ConflictUpdateExpression {
	if c.whereClause == nil {
		c.whereClause = NewExpressionList(AndType, expressions...)
	} else {
		c.whereClause = c.whereClause.Append(expressions...)
	}
	return c
}

// Append to the existing Where clause for an ON CONFLICT DO UPDATE ... WHERE ...
//
//	InsertConflict(DoNothing(),...) -> INSERT INTO ... ON CONFLICT DO NOTHING
func (c *conflictUpdate) WhereClause() ExpressionList {
	return c.whereClause
}
