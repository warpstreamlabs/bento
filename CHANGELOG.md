Changelog
=========

All notable changes to this project will be documented in this file.
 
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
