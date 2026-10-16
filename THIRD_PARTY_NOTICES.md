# Third-party notices

## RelayKit (new-api)

- Module: `github.com/QuantumNous/new-api/relaykit` v0.3.4
- Source: https://github.com/QuantumNous/new-api/tree/main/relaykit
- License: GNU Affero General Public License v3.0 (RelayKit is covered by the
  root `LICENSE` of the new-api repository; see also its `NOTICE`).
- Used as the protocol conversion engine of the request gateway
  (`backend/gateway/protocol/relaykit_engine.go`). It is linked as a Go module
  dependency; no RelayKit source is copied into this repository.

Because RelayKit is AGPL-3.0, distributing UpstreamOps binaries or offering the
gateway to users over a network carries AGPL-3.0 obligations, including making
the corresponding source code available to those users.
