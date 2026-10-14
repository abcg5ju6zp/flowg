# config

The package at `internal/storage/databases/config` implements the
`ConfigStorage` contract from [interfaces](../../interfaces).

It persists the resources that define how FlowG processes logs — pipelines,
transformers and forwarders — together with the system-wide configuration,
exposing them through the `ConfigStorage` interface so the engines and API never
depend on a concrete database. The implementation is backend-agnostic: it runs on
top of any [generic/kv](../../generic/kv) adapter.

## Responsibilities

- **Resource persistence** — stores and retrieves pipelines, transformers and
  forwarders through a key-value adapter, migrating them to the latest model
  version on read when needed.
- **Change notifications** — broadcasts a
  [confignotify](../../../engines/confignotify) event after every successful
  mutation (when a notifier is wired), so the pipeline runner follows
  configuration changes without the API layer driving it. Backends with their
  own change watcher (FoundationDB) pass a nil notifier and emit from the
  watcher instead.
- **System configuration** — persists and caches global settings such as the
  allowed origins used by the ingestion endpoints, validating them on write.
  `InvalidateSystemConfigCache` drops the cache when the stored value changed
  behind it: on restore, or when another node of a cluster wrote it.
- **Snapshots** — satisfies `Streamable` so the configuration database can be
  backed up and restored.
- **Configuration versions** — every pipeline, transformer and forwarder
  mutation is stamped with a strictly increasing, queryable configuration
  version, so the pipeline runner can correlate commits, notifications and
  builds and deduplicate them.

## Configuration versioning

Every successful write or delete of a pipeline, transformer or forwarder
commits three things in a single key-value transaction:

- the data key `<itemType>/<name>` (unchanged layout);
- the global version at `meta/config-version`, incremented by one;
- the per-item version at `meta/item-version/<itemType>/<name>`, stamped with
  the new global value (and cleared on delete).

The notifier event emitted after the commit carries the new global version.
`ReadConfigVersion` and `ListItemVersions` expose the versions; stores without
the meta keys (legacy databases) read as version `0`, which consumers treat as
"unknown" rather than skipping. The system configuration is not part of the
version stream because it does not affect compiled pipelines.

Restore (`Load`) bumps the global version once in a follow-up transaction and
stamps every surviving item with that value, so a restore — including content
that carried smaller versions in the snapshot — is always reconciled, and the
subsequent `DependenciesChanged` event carries the new version.

## Layout

- **storage.go** — the `Storage` type implementing `ConfigStorage`, delegating
  each operation to the `transactions` subpackage inside a read or write
  transaction.
- **[transactions](transactions)** — the low-level read/write operations and the
  config key-space layout.
