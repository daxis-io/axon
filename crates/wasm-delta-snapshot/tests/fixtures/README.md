Synthetic checkpoint and sidecar fixtures for `wasm-delta-snapshot` tests are generated at test
runtime so the repo only needs a lightweight placeholder directory.

Current generated coverage includes:

- classic checkpoints and `_last_checkpoint` selection
- V2 checkpoint manifests and sidecar resolution
- deterministic JSON replay after checkpoint selection
- add-action pruning facts, including optional `stats` payloads
- compatibility cases for supported, native-only, terminal unsupported, and unknown-feature paths
