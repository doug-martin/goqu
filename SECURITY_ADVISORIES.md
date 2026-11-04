# Security advisories for v9.24.0

Both issues are fixed in [v9.24.0](https://github.com/doug-martin/goqu/releases/tag/v9.24.0). Upgrade if you are affected.

---

## SQL injection through string literal escaping in the sqlserver dialect

- Ecosystem: Go, package `github.com/doug-martin/goqu/v9`
- Affected versions: `>= 9.7.0, < 9.24.0`
- Patched version: `9.24.0`
- Severity: high
- CWE: CWE-89

## Impact

When the `sqlserver` dialect generates interpolated (non prepared) SQL, a single quote inside a string value was escaped as `\'`. SQL Server does not treat a backslash as an escape character, so the quote that follows the backslash ended the string literal early. An attacker who controls a string value that an application interpolates into a query can break out of the literal and run arbitrary SQL.

For the value `' OR 1=1 --` the generated literal was `'\' OR 1=1 --'`.

Only applications that use the `sqlserver` dialect and do not use prepared statements (`Prepared(true)`, which is not the default) with untrusted string values are affected. Values sent as prepared statement arguments are not affected.

## Patches

Fixed in v9.24.0. The `sqlserver` dialect now escapes only the single quote, by doubling it (`''`), which is the T-SQL string literal rule.

## Workarounds

Use prepared statements (`Prepared(true)`) for queries that include untrusted values, or validate and escape untrusted values before passing them to goqu.

## References

- Fix: https://github.com/doug-martin/goqu/pull/492
- Issue: https://github.com/doug-martin/goqu/issues/411

---

## SQL injection through unescaped quote characters in quoted identifiers

- Ecosystem: Go, package `github.com/doug-martin/goqu/v9`
- Affected versions: `>= 9.0.0, < 9.24.0`
- Patched version: `9.24.0`
- Severity: medium
- CWE: CWE-89

## Impact

goqu wrote schema, table and column names between the dialect's identifier quote character (a double quote, or a backtick for the mysql and sqlite3 dialects) without escaping a quote character contained in the name. An application that passes untrusted input as an identifier (for example `goqu.C(userInput)`, `goqu.I(userInput)` or `goqu.T(userInput)`) allowed that input to close the quoted identifier and inject arbitrary SQL.

For the identifier ``a" FROM x; --`` the generated SQL was `"a" FROM x; --"`.

This affects interpolated and prepared statements alike, because identifiers are never sent as arguments. Applications that only use fixed identifiers, or validate identifiers against an allow list, are not affected.

## Patches

Fixed in v9.24.0. A quote character inside an identifier is now doubled (`"a"" FROM x; --"`), as the SQL standard and all built in dialects expect.

## Workarounds

Never pass untrusted input as an identifier. Validate identifiers against an allow list of known schema, table and column names.

## References

- Fix: https://github.com/doug-martin/goqu/pull/487
- Issue: https://github.com/doug-martin/goqu/issues/428
