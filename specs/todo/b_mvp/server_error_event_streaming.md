# Server Error Event Streaming — TODO (MVP)

`ServerEvent::Error` type exists in the spec but is never sent by the server. Errors only reported via HTTP status codes on sync requests. Should also push errors on the event stream for client-side debugging.
