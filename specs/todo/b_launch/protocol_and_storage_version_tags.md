# Protocol & Storage Version Tags — TODO (Launch)

Ensure all messages, protocols, and storage formats carry version tags for forward compatibility.

## Overview

Every serialized boundary needs a version indicator so that older clients/servers/storage can detect incompatible data and either migrate or reject gracefully:

- Sync protocol messages (HTTP/SSE payloads, binary sync frames)
- Storage formats (Fjall artifacts, OPFS B-tree pages, manifest/checkpoint data)
- Catalogue entries (schema definitions, lens definitions)
- Worker bridge messages

## Open Questions

- Version tag format — semantic version, monotonic integer, or content hash?
- Migration strategy — inline upgrade on read, or explicit migration step?
- What happens when a client sends a newer protocol version than the server understands?
