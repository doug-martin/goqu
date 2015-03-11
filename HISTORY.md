## v0.2.0

* Changed `CrudExec` to not wrap driver errors in a GoquError [#2](https://github.com/doug-martin/goqu/issues/2)
* Added ability to use a dataset in an `Ex` map or `Eq` expression without having to use `In` [#3](https://github.com/doug-martin/goqu/issues/3)
   * `db.From("test").Where(goqu.Ex{"a": db.From("test").Select("b")})`
* Updated readme with links to [`DefaultAdapter`](https://godoc.org/github.com/doug-martin/goqu#DefaultAdapter)

## v0.1.1

* Added SQLite3 adapter [#1](https://github.com/doug-martin/goqu/pull/1) - [@mattn](https://github.com/mattn)

## v0.1.0

* Added:
    * [`Ex`](https://godoc.org/github.com/doug-martin/goqu#Ex)
    * [`ExOr`](https://godoc.org/github.com/doug-martin/goqu#ExOr)
    * [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op)
* More tests and examples
* Added CONTRIBUTING.md
* Added LICENSE information
* Removed godoc introduction in favor of just maintaining the README.

## v0.0.2

* Fixed issue with goqu.New not returning a pointer to a Database

## v0.0.1

* Initial release