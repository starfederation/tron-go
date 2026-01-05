# tron-go

<picture>
  <img src="assets/logo.png" alt="TRON logo">
</picture>

Go implementation of TRie Object Notation (TRON), a binary JSON-compatible
format backed by HAMT maps and vector tries for copy-on-write updates.

## Spec

- TRON binary format: `tron-shared/SPEC.md` (spec: https://github.com/starfederation/tron/blob/main/SPEC.md)
- HAMT/vector trie overview: `tron-shared/PRIMER.md`

## Implemented features

- ✅ Core TRON encoding/decoding for scalar and tree documents (JSON primitives).
- 🔑 Deterministic map encoding (sorted keys) to preserve canonical ordering.
- 🧵 Append-only trailers with historical roots for copy-on-write updates.
- ⚡ Random-access reads and copy-on-write updates via HAMT maps and vector tries (`MapGet`/`MapSet`/`MapDel`, `ArrGet`/`ArrSet`/`ArrAppend`/`ArrSlice`).
- 🔁 JSON interop (`FromJSON`, `ToJSON`, `WriteJSON`) with `b64:` binary mapping.
- 🧬 Clone helpers for map/array subtrees and values between documents.
- 🧭 JMESPath-style search/compile/transform for TRON docs (`path/`).
- 🧩 JSON Merge Patch (RFC 7386) for TRON docs (`merge/`).
- 🛡️ JSON Schema draft 2020-12 validation for TRON docs (`schema/`), with in-document refs and `AddResourceTRON`.
