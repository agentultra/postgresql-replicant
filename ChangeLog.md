All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0.0] - 2021-07-27

This is a minor update that adds a replication connection type,
updates the root module API with more types, and is just a better
quality of life release.

### Added
- ReplicantConnection
- Connection handling functions to Replicant module
- Message types to main Replicant module

## [0.1.0.1] - 2021-06-22

This is a minor update to our experimental release, fixing some pretty
major issues:

### Fixed
- WalSender on the server hanging up because replicant wasn't sending
  updates
- Minor cleanup, throw better errors instead of printing debug
  information

## [0.1.0.0] - 2021-05-04

This is an experimental release to test out this library and get it
ready and polished for 1.0!

### Added
- Initial release!
- `withLogicalStream`
- Protocol types, serialzation instances, etc
- Logical Replication Messages
- Stream state handling
