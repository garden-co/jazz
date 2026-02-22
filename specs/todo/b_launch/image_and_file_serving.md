# Image & File Serving — TODO

Serve binary assets (images, files) directly from the database over HTTP.

## Overview

Enable the server to serve stored binary data as HTTP responses with correct content types:

- `GET /files/:id` — serve a binary column's content with proper `Content-Type`
- Image transformations: resize, crop, format conversion on the fly (or cached)
- CDN-friendly: correct cache headers, ETags based on content hash
- Access control: respect the same auth rules as row-level queries

Builds on `built_in_file_storage.md` for storage, adds an HTTP serving layer.

## Open Questions

- On-the-fly transforms vs. pre-generated variants?
- Streaming large files vs. buffered responses
- Signed URLs for direct CDN access without proxying through the server?
