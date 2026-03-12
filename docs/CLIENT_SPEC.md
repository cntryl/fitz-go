# Fitz Client Specification

> Provenance: The canonical Fitz client protocol documentation lives in the
> server repository under [`fitz/docs/clients`](../../fitz/docs/clients).
> This SDK-local file is a convenience entry point only and must stay aligned
> with that canonical source.

This repository does not maintain an independent copy of the protocol spec.
To prevent SDK drift, treat the following canonical documents as normative:

- [CLIENT_SPEC.md](../../fitz/docs/clients/CLIENT_SPEC.md) for wire formats,
  message semantics, transport behavior, and client/server boundaries.
- [CLIENT_ACCEPTANCE_CRITERIA.md](../../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
  for conformance requirements and reconnect expectations.
- [CLIENT_IMPLEMENTATION_GUIDE.md](../../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)
  for SDK design guidance and public API patterns.
- [CONNECTION_FLOW.md](../../fitz/docs/clients/CONNECTION_FLOW.md) for the
  client-observable lifecycle from transport connect through reconnect.

## Go SDK Policy

When implementing or reviewing `fitz-go` behavior:

- read the canonical documents above first
- update the canonical docs before changing SDK protocol behavior
- keep this file as a pointer, not a second protocol specification

This file intentionally contains no duplicated wire-format tables or request
examples so the Go SDK cannot drift from the server-owned client docs.
