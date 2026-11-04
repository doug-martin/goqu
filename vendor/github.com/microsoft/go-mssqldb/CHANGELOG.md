# Changelog
## [1.11.2](https://github.com/microsoft/go-mssqldb/compare/v1.11.1...v1.11.2) (2026-09-23)


### Bug Fixes

* accept yes/no for boolean connection string keywords ([#468](https://github.com/microsoft/go-mssqldb/issues/468)) ([0ad75cd](https://github.com/microsoft/go-mssqldb/commit/0ad75cd1f92cbfdd4c148b2369332ace4f208160))

## [1.11.1](https://github.com/microsoft/go-mssqldb/compare/v1.11.0...v1.11.1) (2026-09-23)


### Bug Fixes

* drain response tokens on query error to prevent Tx.Query hang ([#407](https://github.com/microsoft/go-mssqldb/issues/407)) ([#410](https://github.com/microsoft/go-mssqldb/issues/410)) ([1f52296](https://github.com/microsoft/go-mssqldb/commit/1f5229662c8006ab3f2ff8bee49d24faa95287b8))
* preserve strict encryption in DSN URLs ([#452](https://github.com/microsoft/go-mssqldb/issues/452)) ([ae21d6a](https://github.com/microsoft/go-mssqldb/commit/ae21d6a9a7b0d88e22a0ddb957ad792205c4cfdc))


### Reverts

* undo response-cleanup changes from [#410](https://github.com/microsoft/go-mssqldb/issues/410) ([#469](https://github.com/microsoft/go-mssqldb/issues/469)) ([46dbf85](https://github.com/microsoft/go-mssqldb/commit/46dbf85941c5f2bba0cbde962fc0b113834cac9a))

## [1.11.0](https://github.com/microsoft/go-mssqldb/compare/v1.10.0...v1.11.0) (2026-08-23)


### Features

* add ADO.Net connection string synonyms ([#374](https://github.com/microsoft/go-mssqldb/issues/374)) ([d72f750](https://github.com/microsoft/go-mssqldb/commit/d72f750358c4370852fc73cdda9f7f83b53903ff))


### Bug Fixes

* clarify NTLM URL encoding for URL DSNs ([#378](https://github.com/microsoft/go-mssqldb/issues/378)) ([551d622](https://github.com/microsoft/go-mssqldb/commit/551d62231613700aca651cb5b778cdc1db584893))
* close TCP connection on all connect() error paths ([#367](https://github.com/microsoft/go-mssqldb/issues/367)) ([0e13fee](https://github.com/microsoft/go-mssqldb/commit/0e13fee2c0bf0efd38803976171cb30d1bc961ad))
* **isProc:** add `REVERT` builtin command ([#393](https://github.com/microsoft/go-mssqldb/issues/393)) ([31a9659](https://github.com/microsoft/go-mssqldb/commit/31a9659ac4763e622fc9fbae6fcff44f30760164))
* prevent NTLM challenge out-of-bounds panic (DoS) ([#411](https://github.com/microsoft/go-mssqldb/issues/411)) ([81df983](https://github.com/microsoft/go-mssqldb/commit/81df9834c92c7709cdc180c92f8d9a06b9710981))
* prevent panic on overflowing PRELOGIN option offset and length ([#415](https://github.com/microsoft/go-mssqldb/issues/415)) ([802d9f1](https://github.com/microsoft/go-mssqldb/commit/802d9f154ab83d7fb88d56f135f20787f5871f15))
* prevent SQL Browser DAC response parsing panic (DoS) ([#414](https://github.com/microsoft/go-mssqldb/issues/414)) ([bc4cbf2](https://github.com/microsoft/go-mssqldb/commit/bc4cbf28590fba2f19fb0dfe7cf77891dddb1e1d))
* quote table name and ORDER columns in bulk copy ([#416](https://github.com/microsoft/go-mssqldb/issues/416)) ([f925c53](https://github.com/microsoft/go-mssqldb/commit/f925c5343a0fda33276785b765e5571dead35e15))
* replace deprecated reflect.SliceHeader with unsafe.Slice ([#366](https://github.com/microsoft/go-mssqldb/issues/366)) ([836048a](https://github.com/microsoft/go-mssqldb/commit/836048ac320da5b92d1c0f7038ba5ac562768f10)), closes [#272](https://github.com/microsoft/go-mssqldb/issues/272)
* return float64 for REAL columns in readFixedType ([#381](https://github.com/microsoft/go-mssqldb/issues/381)) ([abddba8](https://github.com/microsoft/go-mssqldb/commit/abddba82884f2b672b88088a1849cb547194a27d))
* strip port from AKV URLs for AllowedLocations and endpoint ([#369](https://github.com/microsoft/go-mssqldb/issues/369)) ([7fab98a](https://github.com/microsoft/go-mssqldb/commit/7fab98a2b5b3d57b933a554b6abe15114b5857b3))
* **types:** cap initial PLP buffer allocation ([#409](https://github.com/microsoft/go-mssqldb/issues/409)) ([f088291](https://github.com/microsoft/go-mssqldb/commit/f0882915fbf71953d3251bd7482325dd62924050))


### Performance Improvements

* add comprehensive benchmark suite with CI regression detection ([#376](https://github.com/microsoft/go-mssqldb/issues/376)) ([b935441](https://github.com/microsoft/go-mssqldb/commit/b9354415013929850e1a48856661d1109be661a9))
* fast path for pure ASCII decoding in CharsetToUTF8 ([#431](https://github.com/microsoft/go-mssqldb/issues/431)) ([db51796](https://github.com/microsoft/go-mssqldb/commit/db5179626672ada427888117b399963d2c3a642b))

## [1.10.0](https://github.com/microsoft/go-mssqldb/compare/v1.9.8...v1.10.0) (2026-04-25)


### Features

* add devcontainer for VS Code and GitHub Codespaces ([#317](https://github.com/microsoft/go-mssqldb/issues/317)) ([b55beeb](https://github.com/microsoft/go-mssqldb/commit/b55beebc209f142248f556e3586ce2396be1955c))
* add FailoverPartnerSPN connection string parameter ([#327](https://github.com/microsoft/go-mssqldb/issues/327)) ([ea77c2e](https://github.com/microsoft/go-mssqldb/commit/ea77c2edc7b1c65047cd431975ca38565b567a5f))
* add NewConnectorWithProcessQueryText for mssql driver compatibility ([#341](https://github.com/microsoft/go-mssqldb/issues/341)) ([2be611f](https://github.com/microsoft/go-mssqldb/commit/2be611f8a7b2ec5125a835e1d6efb8cfb8979a86))
* add nullable civil types for date/time parameters ([#325](https://github.com/microsoft/go-mssqldb/issues/325)) ([c10fa99](https://github.com/microsoft/go-mssqldb/commit/c10fa9936a1733ef3f1a50d66b6046445bee3294))


### Bug Fixes

* allow named pipe protocol support for ARM64 Windows ([#232](https://github.com/microsoft/go-mssqldb/issues/232)) ([a82c058](https://github.com/microsoft/go-mssqldb/commit/a82c05866462e56b43d42e4523576aa363a3871a))
* configure release-please with PAT and correct component mapping ([#349](https://github.com/microsoft/go-mssqldb/issues/349)) ([23bac05](https://github.com/microsoft/go-mssqldb/commit/23bac055cea5040891be595e34869b19378974ad))
* detect server-aborted transactions to prevent silent auto-commit (XACT_ABORT) ([#370](https://github.com/microsoft/go-mssqldb/issues/370)) ([586ea53](https://github.com/microsoft/go-mssqldb/commit/586ea53b337210693883d554f12a6b9c07e2cca2))
* expose TrustServerCertificate in msdsn.Config and URL round-trip ([#312](https://github.com/microsoft/go-mssqldb/issues/312)) ([9937cfe](https://github.com/microsoft/go-mssqldb/commit/9937cfe437d437d86d96b347514cd1ccbb5485f9))
* handle COLINFO (0xA5) and TABNAME (0xA4) TDS tokens returned by tables with triggers ([#343](https://github.com/microsoft/go-mssqldb/issues/343)) ([7c905ad](https://github.com/microsoft/go-mssqldb/commit/7c905adac4e8e00856c3d20503902f0160f5853d))
* implement driver.DriverContext interface ([#365](https://github.com/microsoft/go-mssqldb/issues/365)) ([1b610a0](https://github.com/microsoft/go-mssqldb/commit/1b610a0b2905dc472968842e7a8f252acdb94272)), closes [#236](https://github.com/microsoft/go-mssqldb/issues/236)
* make readCancelConfirmation respect context cancellation ([#359](https://github.com/microsoft/go-mssqldb/issues/359)) ([65e137f](https://github.com/microsoft/go-mssqldb/commit/65e137f4896c9f3de6036967afe4722ad6a21a41))
* replace broken AppVeyor badge with GitHub Actions badge ([#334](https://github.com/microsoft/go-mssqldb/issues/334)) ([d3429f5](https://github.com/microsoft/go-mssqldb/commit/d3429f5bb895bdb884ae39a43d87bb384aa5353a))
* return interface{} scanType for sql_variant instead of nil ([#362](https://github.com/microsoft/go-mssqldb/issues/362)) ([296a83a](https://github.com/microsoft/go-mssqldb/commit/296a83a3e25fc23add1dbb243df1a716f431de66)), closes [#186](https://github.com/microsoft/go-mssqldb/issues/186)
* sanitize credentials from connection string parsing errors ([#319](https://github.com/microsoft/go-mssqldb/issues/319)) ([93f5ef0](https://github.com/microsoft/go-mssqldb/commit/93f5ef0dd5f02c9094a22d0052fb95cd553ba971))
* surface server errors from Rows.Close() during token drain ([#361](https://github.com/microsoft/go-mssqldb/issues/361)) ([ea69792](https://github.com/microsoft/go-mssqldb/commit/ea69792c6da6d049eaec4e672a95d55bafef48f5)), closes [#244](https://github.com/microsoft/go-mssqldb/issues/244)

## 1.9.6

### Features

* Added new `serverCertificate` connection parameter for byte-for-byte certificate validation, matching Microsoft.Data.SqlClient behavior. This parameter skips hostname validation, chain validation, and expiry checks, only verifying that the server's certificate exactly matches the provided file. This is useful when the server's hostname doesn't match the certificate CN/SAN. (#304)
* The existing `certificate` parameter maintains backward compatibility with traditional X.509 chain validation including hostname checks, expiry validation, and chain-of-trust verification.
* `serverCertificate` cannot be used with `certificate` or `hostnameincertificate` parameters to prevent conflicting validation methods.

## 1.9.3

### Bug fixes

* Fix parsing of ADO connection strings with double-quoted values containing semicolons (#282)

## 1.9.2

### Bug fixes

* Fix race condition in message queue query model (#277)

## 1.9.1

### Bug fixes

* Fix bulk insert failure with datetime values near midnight due to day overflow (#271)
* Fix: apply guidConversion option in TestBulkcopy (#255)

### Features

* support configuring custom time.Location for datetime encoding and decoding via DSN (#260)
* Implement support for the latest Azure credential types in the azuread package (#269)

## 1.8.2

### Bug fixes

* Added "Pwd" as a recognized alias for "Password" in connection strings (#262)
* Updated `isProc` to detect more keywords

## 1.7.0

### Changed

* Changed always encrypted key provider error handling not to panic on failure

### Features

* Support DER certificates for server authentication (#152)

### Bug fixes

* Improved speed of CharsetToUTF8 (#154)

## 1.7.0

### Changed

* krb5 authenticator supports standard Kerberos environment variables for configuration

## 1.6.0

### Changed

* Go.mod updated to Go 1.17
* Azure SDK for Go dependencies updated

### Features

* Added `ActiveDirectoryAzCli` and `ActiveDirectoryDeviceCode` authentication types to `azuread` package
* Always Encrypted encryption and decryption with 2 hour key cache (#116)
* 'pfx', 'MSSQL_CERTIFICATE_STORE', and 'AZURE_KEY_VAULT' encryption key providers
* TDS8 can now be used for connections by setting encrypt="strict"

## 1.5.0

### Features

### Bug fixes

* Handle extended character in SQL instance names for browser lookup (#122)

## 1.4.0

### Features

* Adds UnmarshalJSON interface for UniqueIdentifier (#126)

### Bug fixes

* Fixes MarshalText prototype for UniqueIdentifier

## 1.2.0

### Features

* A connector's dialer can now be used to resolve DNS if the dialer implements the `HostDialer` interface

## 1.0.0

### Features

* `admin` protocol for dedicated administrator connections

### Changed

* Added `Hidden()` method to `ProtocolParser` interface

## 0.21.0

### Features

* Updated azidentity to 1.2.1, which adds in memory cache for managed credentials ([#90](https://github.com/microsoft/go-mssqldb/pull/90))

### Bug fixes

* Fixed uninitialized server name in TLS config ([#93](https://github.com/microsoft/go-mssqldb/issues/93))([#94](https://github.com/microsoft/go-mssqldb/pull/94))
* Fixed several kerberos authentication usages on Linux with new krb5 authentication provider. ([#65](https://github.com/microsoft/go-mssqldb/pull/65))

### Changed

* New kerberos authenticator implementation uses more explicit connection string parameters.

| Old          | New                |
|--------------|--------------------|
| krb5conffile | krb5-configfile    |
| krbcache     | krb5-credcachefile |
| keytabfile   | krb5-keytabfile    |
| realm        | krb5-realm         |

## 0.20.0

### Features

* Add driver version and name to TDS login packets
* Add `pipe` connection string parameter for named pipe dialer
* Expose network errors that occur during connection establishment. Now they are
wrapped, and can be detected by using errors.As/Is practise. This connection
errors can, and could even before, happen anytime the sql.DB doesn't have free
connection for executed query.

### Bug fixes

* Added checks while reading prelogin for invalid data ([#64](https://github.com/microsoft/go-mssqldb/issues/64))([86ecefd8b](https://github.com/microsoft/go-mssqldb/commit/86ecefd8b57683aeb5ad9328066ee73fbccd62f5))

* Fixed multi-protocol dialer path to avoid unneeded SQL Browser queries
