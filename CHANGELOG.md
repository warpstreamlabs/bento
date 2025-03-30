Changelog
=========

All notable changes to this project will be documented in this file.

## 1.6.0 - 2025-03-30

### Changed 

- experimental 'strict' / 'retry' error handling modes will be overridden in presence of incompatible components/bloblang @gregfurman & @jem-davies
- discord `bot_token` field marked as secret @jem-davies
- `golang.org/x/exp/rand` replaced with `math/rand/v2` @Juneezee
- AWS tests refactored to use LocalStack fixture and pro-token @gregfurman
- update JWT dependency @gitphill

### Added 

- field `ws_message_type` in output `http_server` @cheparinV
- experimental 'retry' error handling mode @gregfurman
- fields `backoff_duration` and `start_seq_num` to `s2` input @vrongmeal
- input component `cypher` @jem-davies
- field `auto_offset_reset` added to `kafka_franz` input, matching Kafka's `auto.offset.reset` config for when a consumer group has no initial committed consumer offset available @gregfurman
- fields `expected_headers` and `expected_number_of_fields` to `csv`  scanner @jem-davies
- fields `update_visibility` and `custom_request_headers` to `sqs` input @gregfurman

### Fixed

- MQTT integration tests @gregfurman
- GCP integration tests @gregfurman
- GCP PubSub integration tests @jem-davies
- Kafka integration tests @gregfurman
- metadata creation in `sqs` input to now include all message attributes @gregfurman

## 1.5.2 - 2025-03-12

- upgrade to go 1.23 @gitphill
- various dependency updates for CVEs @gitphill

## 1.5.1 - 2025-02-14

### Fixed 

- check for shutdown before using fallback output component @jem-davies
- only load aws cred if using postgres + secretName in sql components @jem-davies

## 1.5.0 - 2025-02-07

### Changed

- log message format in `gcp_bigquery_write_api` @richardartoul
- enabled bloblang interpolation in `gcp_cloud_storage` input @paulosimao-ardanlabs
- add metadata values to `nats_kv` processor @kmpm

### Added

- `rate_limit` field in `kafka_franz` input @gregfurman
- `unsafe_dynamic_query` added to `gcp_bigquery_select` processor @jem-davies
- `delete_objects` added to `aws_s3` processor @gregfurman
- bloblang string method `repeat` @jem-davies
- plugin example module `./resources/plugin-example` @jem-davies
- zeromq input & output components @kmpm
- tls config field for `cypher` output @jem-davies
- `s2` input & output components @vrongmeal
- 'sampling bundle' for logging @gregfurman

### Fixed 

- replace directive for github.com/AthenZ/athenz depenency @gregfurman
- change default encoding from DELTA_LENGTH_BYTE_ARRAY to PLAIN for string fields for parquet encodings @gregfurman

## 1.4.1 - 2025-01-04

### Changed

- updated various dependencies to address CVEs @jbeemster

## 1.4.0 - 2024-12-08

### Added

- Rate limiting functionality to rate-limit based on bytes @gregfurman
- `error_handling.strategy` config field to override Bento's default error handling @gregfurman
- Experimental `aws_s3` processor  @jem-davies
- Experimental `log_all_errors` field to `logger` config that promotes any log containing an error type to the ERROR level @gregfurman
- `batch_policy.jitter` field to add a random delay to batch flush intervals @gregfurman
- `strategy` field to `dedupe` processor @gregfurman
- Experimental `cypher` output component @jem-davies

### Changed

- updated opensearch-go to v4 @arnitolog
- updated the go.mod file to specify Go version 1.22

### Fixed

- Doc site search indexing @gregfurman
- Removed unused benthos studio package @gregfurman
- Error handling issue with `opensearch` output @arnitolog
- incorrect reference to `restart_on_close` in `subprocess` processor docs @dacalabrese
- "zombie" http output websocket connections @buger
- correctly set Bento binary information at build-time in CI @jem-davies

## 1.3.0 - 2024-11-04

### Added

- Exposed option to enable event flushing for processor events & input events @sananguliyev
- Added `QuestDB` output component @sklarsa
- Added `spanner` driver to SQL components @rvilim
- Added more config fields to `kafka_franz` input component @gregfurman
- Added `gcp_bigquery_write_api` output component @gregfurman
- Added AWS config fields to SQL Components to enable storing user/password in AWS Secret Manager @jem-davies
- Added more types to `parquet_encode` processor @ryanworl
- Altered default value for field `cas_enabled` to true in `couchbase` processor @sapk

### Changed

- Improved parquet handling of optional decimals and float <> float conversions @richardartoul
- Altered `gcp_bigquery` config field `table` to allow interpolation @jem-davies

## 1.2.0 - 2024-08-21

### Added

- avro_schema_nested field to registry_schema_decode/encode @jem-davies
- etcd-component input for watching an etcd key or prefix @gregfurman
- Couchbase: add CAS support and disable by default @sapk
- sql: Add optional verification ping on database client initialisation @gregfurman

### Upstream Changes

- [v4.31.0 - 2024-07-18](./CHANGELOG.old.md#4.31.0-2024-07-18)  

## 1.1.0 - 2024-07-12

### Added

- Bento mascot as a `favicon.ico` to docusaurus site and back into the `README`.
- New cookbook for Kafka topic mirroring.
- New local testing guide for `bento-lambda`.

### Changed

- Removed more references to upstream in documentation.

### Upstream Changes Synced

- [v4.30.0 - 2024-05-29](./CHANGELOG.old.md#4.30.0-2024-06-13)    
- [v4.29.0 - 2024-05-29](./CHANGELOG.old.md#4.29.0-2024-06-10)    
- [v4.28.0 - 2024-05-29](./CHANGELOG.old.md#4.28.0-2024-05-29)    

## 1.0.2 - 2024-06-07

### Changed

- Updated repository documentation.

### Fixed

- Removed referenced to upstream `v4` in favour of Bento `v1`.
- Fixed flag in Docker `streams_mode` resource.

## 1.0.1 - 2024-06-03

### Changed

Using github's container registry instead of AWS ECR.

## 1.0.0 - 2024-06-03

First release after the fork, pushing binaries to the new github repo and docker images to an AWS ECR.
