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



