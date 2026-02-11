# Binary Columns & FK Refs Instead of Blobs — TODO

Replace opaque blob storage with typed binary columns and proper foreign key references.

## Overview

Currently large binary data is stored as blobs. Instead, binary columns should be first-class column types with FK references pointing to the binary data. This enables:

- Schema-aware binary data (images, files, etc.) with proper typing
- FK integrity between rows and their binary attachments
- Efficient sync — binary data can be transferred separately from row metadata
- Garbage collection of orphaned binary data via FK tracking

## Open Questions

- Chunking strategy for large binaries (content-addressed chunks?)
- How do binary columns interact with lenses/schema migration?
- Lazy loading semantics — should binary data be fetched on demand or eagerly synced?
- Deduplication across rows referencing identical content
