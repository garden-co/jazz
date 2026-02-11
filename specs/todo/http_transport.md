# HTTP/SSE Transport — TODO

Remaining work items and future enhancements.

> Status quo: [specs/status-quo/http_transport.md](../status-quo/http_transport.md)

## JWKS Support

**Priority: Medium**

Currently only HMAC secret for JWT validation. JWKS (JSON Web Key Sets) needed for production with key rotation and caching.

> `crates/jazz-cli/src/middleware/auth.rs:250` — TODO comment

## Exponential Backoff

**Priority: Low**

Reconnection uses fixed 5s delay. Should implement exponential backoff with jitter.

> `crates/jazz-rs/src/client.rs:252`

## Server-Side Error Events

**Priority: Low**

`ServerEvent::Error` type exists in the spec but is never sent by the server. Errors only reported via HTTP status codes on sync requests. Could improve client-side debugging to also push errors on the event stream.

## Future Enhancements

- WebSocket alternative to SSE
- Binary protocol option (MessagePack/CBOR) to replace JSON within frames
- Request signing/verification
- CORS configuration beyond permissive dev mode
