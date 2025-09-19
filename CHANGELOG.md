Changelog
=========

All notable changes to this project will be documented in this file.

## 1.11.0 - 2025-09-19

### Added

- new bloblang method `split_by`, similar to `split` but takes a predicate rather than a value for the delimiter @iamramtin
- `nlp_classify_text`, `nlp_classify_tokens`, `nlp_extract_features` & `nlp_zero_shot_classify` processors enable use of ONNX NLP models @gregfurman
- `elasticsearch_v2` output component, uses official elasticsearch go client @jem-davies
- `gcp_spanner_cdc` consumes Spanner Change Stream Events from a GCP Spanner instance @anicoll & @gregfurman

### Changed

- deprecated `elasticsearch` output @jem-davies
- deprecated `parquet` processor removed @jem-davies
- `golangci-lint` upgraded to V2 @miparnisari
- bloblang method `split` can now operate on arrays as well as strings @iamramtin

### Fixed

- fix input `azure_blob_storage` failing to delete blobs when using `targets_input` & `delete_object: true` @adrianhag
- fix `inject_tracing_map` config examples @eastnine90
- fix data-race in config/schema.go @miparnisari
- fix data-race in sql dsn building @gregfurman
- fix validation for the seed_brokers config field in `kafka_franz` @gregfurman
- fix oracle integration tests @jem-davies

## 1.10.2 - 2025-09-15

### Fixed 

- `sql_insert` & `sql_raw` output components check connection, and will attempt reconnection via Components' Connect() func @jem-davies

## 1.10.1 - 2025-09-10

### Fixed 

 - fix data-race in sql dsn building @gregfurman
 - fix issue connecting to RDS via IAM Auth @gregfurman

## 1.10.0 - 2025-08-14

### Added

- `SharedMetricsSetup` added to service package enabling multiple streams created with StreamBuilder to share the same metric registry @ecordell
- `azure` fields added to SQL components enabling using Azure based auth for Azure Postgres SQL @jem-davies
- `aws_dynamodb` output also supports deleting items from a dynamodb table @rohankumardubey & @jem-davies
- WASM bloblang playground to doc site @iamramtin
- reconnect config options to `zmq4n` output @gregfurman
- `MockResourcesOptUseSlogger` function to enable setting a logger on MockResources @jem-davies

### Changed

- Bento will emit warning logs if a config is using a deprecated component/field @jem-davies
- CSV scanner & input will now error if a config sets `parse_header_row` is false & `expected_headers` is non-empty @rohankumardubey

## 1.9.1 - 2025-07-29

### Fixed 

- multipart s3 uploads for `aws_s3` output destination @Towerthousand

## 1.9.0 - 2025-07-07

- update `parquet-go` dependency from `0.23.0` to `0.24.0` @gregfurman

### Added 

- `create` added to `opensearch`'s `action` field to support write to data streams @arnitolog
- `message_format` to `gcp_bigquery_write_api` enabling protobuf messages to be sent without need for marshalling to json @gregfurman
- `use_parquet_list_format` flag to `parquet_decode` allowing a `LIST` column to return as either a Parquet logical type or Go slice @gregfurman

### Changed 

- Full rework of local bloblang editor/playground to be shinier, prettier, and more interactive @iamramtin
- metadata `gcp_pubsub_message_id` & `gcp_pubsub_ordering_key` added to `gcp_pubsub` input @anicoll

## 1.8.2 - 2025-06-25

### Fixed 

- `aws_cloudwatch` metrics not flushed before Bento shutdown @jem-davies

## 1.8.1 - 2025-06-17

### Added 

- `AWS_MSK_IAM` option to `kafka` components `sasl.mechanism` @gitphill

## 1.8.0 - 2025-06-08

### Added 

- `iam_enabled` field to sql components enables connection to aws rds instances via iam authentication @gregfurman & @jem-davies
- `nats_object_store` components enable connecting to a nats object store @jem-davies
- `reconnect_on_unknown_topic` field enables the `kafka_franz` input to handle recreated topics @brianshih1
- `expected_headers` & `expected_number_of_fields` added to `csv` input providing more validation options @etolbakov

### Fixed 

- `elasticsearch` output `tls` connections @jem-davies

### Changed 

- `open_message` field in `websocket` input deprecated for `open_messages` enabling sending multiple messages to the server upon connection @jr200
- `ResourceFromYAML` test helpers to use constant during string formatting, in preparation for Go 1.24 update @gregfurman

## 1.7.1 - 2025-05-13 

- enabled `error_handling.strategy` fields when using streamBuilder.Build() @MaudGautier
- update gosnowflake dependency and fix test @gregfurman

## 1.7.0 - 2025-05-07

### Added 

- `xml_documents_to_json` & `xml_documents_to_json:cast` options to `unarchive` processor `format` field @lublak 
- `xml_documents` scanner - consumes a stream of one or more XML documents into discrete messages @lublak
- `bsr` fields to `protobuf` processor - enables loading & using schemas from a remote "Buf Schema Registry" @oliver-anz
- `stream_format` and `heartbeat` fields to `http_server` output to emit Server-Side Events (SSE) @asoorm

### Fixed 

- ignoring context cancellation / timeout during graceful shutdown of streams for error logging purposes @jub0bs 
- public implementations of `MessageAwareRateLimit` now correctly register @MaudGautier

## 1.6.1 - 2025-05-06

- update apache/pulsar-client-go dependency for CVE @gregfurman

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
