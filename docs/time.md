# Working with time.Time

By default when interpolating `time.Time` (and `*time.Time`) `goqu` will convert it `UTC` before interpolating.

## Why?

For most use cases `UTC` should be preferred, if a timezone is specified it is usually ignored silently by `postgres` and `mysql` unless you configure your DB to run in a different timezone, leading to unexpected behavior.

## How to use a different default timezone?
`goqu` provides a **_global_** configuration settings to set the [location](https://golang.org/pkg/time/#Location) to convert all timestamps to. 

To change the default timezone to covert time instances to you can use [`goqu.SetTimeLocation`](https://godoc.org/github.com/doug-martin/goqu#SetTimeLocation) to change the default timezone.

In the following example the default value `UTC` is used.

```go
created, err := time.Parse(time.RFC3339, "2019-10-01T15:01:00Z")
if err != nil {
	panic(err)
}

ds := goqu.Insert("test").Rows(goqu.Record{
	"address": "111 Address",
	"name":    "Bob Yukon",
	"created": created,
})
```

Output:
```
INSERT INTO "test" ("address", "created", "name") VALUES ('111 Address', '2019-10-01T15:01:00Z', 'Bob Yukon')
```

In the following example `UTC` is overridden to `Asia/Shanghai`

```go
loc, err := time.LoadLocation("Asia/Shanghai")
if err != nil {
	panic(err)
}

goqu.SetTimeLocation(loc)

created, err := time.Parse(time.RFC3339, "2019-10-01T15:01:00Z")
if err != nil {
	panic(err)
}

ds := goqu.Insert("test").Rows(goqu.Record{
	"address": "111 Address",
	"name":    "Bob Yukon",
	"created": created,
})
```

Output:
```
INSERT INTO "test" ("address", "created", "name") VALUES ('111 Address', '2019-10-01T23:01:00+08:00', 'Bob Yukon')
```

## What about time values read from the database?

`SetTimeLocation` only affects `time.Time` values that `goqu` writes into SQL. It has no effect on values you scan back out with `ScanStruct`, `ScanStructs` or `ScanVal`. Those are produced by your database driver, so the location of scanned times is controlled by the driver and column type, for example:

* MySQL: `parseTime=true&loc=Local` (or another location) in the DSN.
* PostgreSQL: `timestamp with time zone` columns return times in the session time zone, while `timestamp without time zone` columns have no zone and are returned as UTC.
* If you need a specific location, convert after scanning with `t.In(loc)`.

## Working with time.Duration

`time.Duration` has underlying type `int64`, so `goqu` encodes it as nanoseconds (`int64`) by default—matching `database/sql` (`driver.DefaultParameterConverter`) and round-tripping cleanly with `BIGINT` columns across drivers.

### PostgreSQL `interval` columns

PostgreSQL `interval` columns have microsecond resolution and decode as text on drivers such as `lib/pq` (which `database/sql` cannot scan directly into `int64`-backed `time.Duration`). To target an `interval` column for specific fields or expressions without affecting `BIGINT` columns:

* **`pgx/v5` (`pgtype.Interval` or opaque `time.Duration`):** `pgtype.Interval` (`github.com/jackc/pgx/v5/pgtype`) implements both `driver.Valuer` and `sql.Scanner`, so it works in prepared and interpolated statements as well as `ScanStruct`:

  ```go
  pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
  ```

  When using `pgx/v5/stdlib` with `Prepared(true)`, you can also keep `time.Duration` on your struct and mark it [opaque](./struct_tags.md#opaque-types) (`goqu:"opaque"` or `goqu.RegisterOpaque[time.Duration]()`) so `goqu` passes the `time.Duration` value unchanged to `pgx` instead of converting it to `int64` (scanning `interval` directly back into `time.Duration` via `database/sql` requires Go 1.27+ `driver.RowsColumnScanner` support in `pgx/v5/stdlib`).

* **Custom `driver.Valuer` (any driver, prepared or interpolated):**

  ```go
  type IntervalDuration time.Duration

  func (d IntervalDuration) Value() (driver.Value, error) {
  	return fmt.Sprintf("%d us", time.Duration(d).Microseconds()), nil
  }
  ```

  **NOTE** Avoid `time.Duration.String()` when formatting PostgreSQL `interval` literals: PostgreSQL rejects `'500µs'` and `'500ns'`, and parses negative compound Go durations such as `"-1h30m0s"` as `-1 hour + 30 minutes` (`-00:30:00`) instead of `-01:30:00`. Formatting microseconds with `"%d us"` avoids all three. To scan an `interval` back on drivers like `lib/pq`, implement `sql.Scanner` on the wrapper type or project microseconds/nanoseconds in SQL (e.g. `EXTRACT(EPOCH FROM col) * 1000000000`).

* **Ad-hoc expressions:**

  ```go
  goqu.L("? * INTERVAL '1 microsecond'", d.Microseconds())
  ```

