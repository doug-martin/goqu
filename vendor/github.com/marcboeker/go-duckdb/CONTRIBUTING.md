# Contributing

### Contributions

* Are you fixing a bug or adding a new feature - Great!
* Announce that you are working on an issue by replying to it. 
Open a new issue to announce that you are working on a new feature.
* Try to avoid large pull requests - they are much harder to review. 

### Pull Requests

* Ensure the PR description clearly describes the problem and solution.
Include the relevant issue number if applicable.
* Add tests to cover your fix or new functionality.
* Pull requests need to pass all continuous integration checks before merging.
* We reserve full and final discretion over whether we merge a pull request or not. 
Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

### Testing

`go-duckdb` aims to contain tests that are fast, cover a significant portion of the code, 
are comprehensible, and can serve as documentation.

### Go Guidelines

go-duckdb uses [Effective Go](https://go.dev/doc/effective_go) and the 
[Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md) 
for tips for writing clear, idiomatic Go code. Below are a few excerpts.

* **Error Naming.**
For error values stored as global variables, use the prefix `Err` or `err`. 
For custom error types, use the suffix `Error` instead.
* **Commentary.**
Comments for humans are formatted like so: `// Uppercase sentence. Single-space next sentence.`
* **Lines.** 
Avoid overly long lines. You can aim to wrap lines that exceed a soft line 
length limit of 100 characters.
* **Naming.**
Choose descriptive, concise names. Avoid single-letter variable names.
* Avoid unnecessary interfaces, only export relevant functions.

## Upgrading DuckDB

To upgrade to a new version of DuckDB:

1. Fork the project and create a new branch. 
2. Change `DUCKDB_BRANCH` in the `Makefile` to match the latest DuckDB version, for example `DUCKDB_BRANCH=v0.10.0`.
3. Push the updated `Makefile` and create a PR.
4. Wait for GitHub Actions to pre-compile the static libraries in `deps`. 
They will be committed automatically to your branch.
5. If everything looks good, we will merge the PR.