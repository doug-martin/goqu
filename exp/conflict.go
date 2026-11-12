package exp

type (
	// conflictTarget holds the optional target of an ON CONFLICT clause. It is shared by the DO NOTHING and
	// DO UPDATE conflict expressions.
	conflictTarget struct {
		cols       ColumnListExpression
		constraint string
		where      ExpressionList
	}
	doNothingConflict struct {
		conflictTarget
	}
	// ConflictUpdate is the struct that represents the UPDATE fragment of an
	// INSERT ... ON CONFLICT/ON DUPLICATE KEY DO UPDATE statement
	conflictUpdate struct {
		conflictTarget
		target      string
		update      any
		whereClause ExpressionList
	}
)

func (t conflictTarget) cloneTarget() conflictTarget {
	clone := conflictTarget{constraint: t.constraint}
	if t.cols != nil {
		clone.cols = t.cols.Clone().(ColumnListExpression)
	}
	if t.where != nil {
		clone.where = t.where.Clone().(ExpressionList)
	}
	return clone
}

func (t conflictTarget) Cols() ColumnListExpression {
	return t.cols
}

func (t conflictTarget) Constraint() string {
	return t.constraint
}

func (t conflictTarget) TargetWhereClause() ExpressionList {
	return t.where
}

func (t *conflictTarget) appendWhere(expressions ...Expression) {
	if t.where == nil {
		t.where = NewExpressionList(AndType, expressions...)
	} else {
		t.where = t.where.Append(expressions...)
	}
}

// Creates a conflict struct to be passed to OnConflict to ignore constraint errors.
//
// The builder methods (SetCols, TargetCols, OnConstraint, TargetWhere) modify and return the receiver. Create a new
// expression (or call Clone) rather than deriving variants from a shared one; InsertDataset.OnConflict clones the
// expression it is given.
//
//	OnConflict(DoNothing()) -> INSERT INTO ... ON CONFLICT DO NOTHING
func NewDoNothingConflictExpression() ConflictNothingExpression {
	return &doNothingConflict{}
}

func (c *doNothingConflict) Expression() Expression {
	return c
}

func (c *doNothingConflict) Clone() Expression {
	return &doNothingConflict{conflictTarget: c.cloneTarget()}
}

func (c *doNothingConflict) Action() ConflictAction {
	return DoNothingConflictAction
}

// Sets the conflict target columns. Only supported by dialects that support a conflict target (e.g. Postgres,
// SQLite); other dialects return an error when generating SQL.
//
//	DoNothing().SetCols(exp.NewColumnListExpression("a")) -> ON CONFLICT ("a") DO NOTHING
func (c *doNothingConflict) SetCols(cl ColumnListExpression) ConflictNothingExpression {
	c.cols = cl
	return c
}

// Sets the conflict target columns from column names or expressions. Only supported by dialects that support a
// conflict target (e.g. Postgres, SQLite); other dialects return an error when generating SQL.
//
//	DoNothing().TargetCols("a", "b") -> ON CONFLICT ("a", "b") DO NOTHING
func (c *doNothingConflict) TargetCols(cols ...any) ConflictNothingExpression {
	return c.SetCols(NewColumnListExpression(cols...))
}

// Sets the name of the constraint to use as the conflict target. Only supported by dialects that support
// ON CONSTRAINT (e.g. Postgres); other dialects return an error when generating SQL. It cannot be combined with
// SetCols or TargetCols. An empty name clears the constraint.
//
//	DoNothing().OnConstraint("users_email_key") -> ON CONFLICT ON CONSTRAINT "users_email_key" DO NOTHING
func (c *doNothingConflict) OnConstraint(name string) ConflictNothingExpression {
	c.constraint = name
	return c
}

// Appends to the partial index predicate of the conflict target. It requires target columns (see TargetCols or
// SetCols).
//
// Values in TargetWhere are always interpolated as SQL literals, even when Prepared(true) is set on the dataset,
// because databases match this predicate statically against a partial unique index definition. Only pass static schema
// constants here — never untrusted user input.
//
//	DoNothing().TargetCols("a").TargetWhere(Ex{"b": nil}) ->
//		ON CONFLICT ("a") WHERE ("b" IS NULL) DO NOTHING
func (c *doNothingConflict) TargetWhere(expressions ...Expression) ConflictNothingExpression {
	c.appendWhere(expressions...)
	return c
}

// Creates a ConflictUpdate struct to be passed to OnConflict.
// Represents an ON CONFLICT DO UPDATE portion of an INSERT statement (ON DUPLICATE KEY UPDATE for mysql).
//
// The builder methods (SetCols, TargetCols, OnConstraint, TargetWhere, Where) modify and return the receiver. Create a
// new expression (or call Clone) rather than deriving variants from a shared one; InsertDataset.OnConflict clones the
// expression it is given.
//
//	OnConflict(DoUpdate("target_column", update)) ->
//		INSERT INTO ... ON CONFLICT (target_column) DO UPDATE SET a=b
//	OnConflict(DoUpdateSet(update).TargetCols("a", "b").Where(Ex{"a": 1})) ->
//		INSERT INTO ... ON CONFLICT ("a", "b") DO UPDATE SET a=b WHERE ("a" = 1)
func NewDoUpdateConflictExpression(target string, update any) ConflictUpdateExpression {
	return &conflictUpdate{target: target, update: update}
}

func (c *conflictUpdate) Expression() Expression {
	return c
}

func (c *conflictUpdate) Clone() Expression {
	clone := &conflictUpdate{
		conflictTarget: c.cloneTarget(),
		target:         c.target,
		update:         c.update,
	}
	if c.whereClause != nil {
		clone.whereClause = c.whereClause.Clone().(ExpressionList)
	}
	return clone
}

func (c *conflictUpdate) Action() ConflictAction {
	return DoUpdateConflictAction
}

// Returns the legacy raw target conflict string passed to DoUpdate.
//
// The target is written as is; prefer DoUpdateSet with TargetCols or OnConstraint, which quote identifiers and support
// multiple columns. Combining a non-empty TargetColumn with TargetCols/SetCols or OnConstraint returns an error.
func (c *conflictUpdate) TargetColumn() string {
	return c.target
}

// Sets the conflict target columns. Only supported by dialects that support a conflict target (e.g. Postgres,
// SQLite); other dialects return an error when generating SQL. Cannot be combined with OnConstraint or a non-empty
// target string passed to DoUpdate.
//
//	DoUpdateSet(update).SetCols(exp.NewColumnListExpression("a", "b")) -> ON CONFLICT ("a", "b") DO UPDATE SET ...
func (c *conflictUpdate) SetCols(cl ColumnListExpression) ConflictUpdateExpression {
	c.cols = cl
	return c
}

// Sets the conflict target columns from column names or expressions. Only supported by dialects that support a
// conflict target (e.g. Postgres, SQLite); other dialects return an error when generating SQL. Cannot be combined with
// OnConstraint or a non-empty target string passed to DoUpdate.
//
//	DoUpdateSet(update).TargetCols("a", "b") -> ON CONFLICT ("a", "b") DO UPDATE SET ...
func (c *conflictUpdate) TargetCols(cols ...any) ConflictUpdateExpression {
	return c.SetCols(NewColumnListExpression(cols...))
}

// Sets the name of the constraint to use as the conflict target. Only supported by dialects that support
// ON CONSTRAINT (e.g. Postgres); other dialects return an error when generating SQL. Cannot be combined with
// SetCols, TargetCols, or a non-empty target string passed to DoUpdate. An empty name clears the constraint.
//
//	DoUpdateSet(update).OnConstraint("users_email_key") ->
//		ON CONFLICT ON CONSTRAINT "users_email_key" DO UPDATE SET ...
func (c *conflictUpdate) OnConstraint(name string) ConflictUpdateExpression {
	c.constraint = name
	return c
}

// Appends to the partial index predicate of the conflict target. It requires target columns (see TargetCols or
// SetCols).
//
// Values in TargetWhere are always interpolated as SQL literals, even when Prepared(true) is set on the dataset,
// because databases match this predicate statically against a partial unique index definition. Only pass static schema
// constants here — never untrusted user input (use Where to filter updated rows with bind parameters).
//
//	DoUpdateSet(update).TargetCols("a").TargetWhere(Ex{"b": nil}) ->
//		ON CONFLICT ("a") WHERE ("b" IS NULL) DO UPDATE SET ...
func (c *conflictUpdate) TargetWhere(expressions ...Expression) ConflictUpdateExpression {
	c.appendWhere(expressions...)
	return c
}

// Returns the Updates which represent the ON CONFLICT DO UPDATE portion of an insert statement. If nil,
// there are no updates.
func (c *conflictUpdate) Update() any {
	return c.update
}

// Append to the existing Where clause for an ON CONFLICT DO UPDATE ... WHERE ...
//
//	OnConflict(DoUpdateSet(update).TargetCols("a").Where(Ex{"b": 1})) ->
//		INSERT INTO ... ON CONFLICT ("a") DO UPDATE SET ... WHERE ("b" = 1)
func (c *conflictUpdate) Where(expressions ...Expression) ConflictUpdateExpression {
	if c.whereClause == nil {
		c.whereClause = NewExpressionList(AndType, expressions...)
	} else {
		c.whereClause = c.whereClause.Append(expressions...)
	}
	return c
}

// Returns the Where clause of the ON CONFLICT DO UPDATE ... WHERE ...
func (c *conflictUpdate) WhereClause() ExpressionList {
	return c.whereClause
}
