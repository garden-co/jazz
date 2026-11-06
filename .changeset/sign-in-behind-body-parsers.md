---
"jazz-tools": patch
---

Signing in works behind a host's JSON body parser. The browser asked the backend who is signed in with an empty POST typed `application/json`: Fastify refused it with 400 (`FST_ERR_CTP_EMPTY_JSON_BODY`), and `express.json()` mounted before `jazz.nodeHandler` swallowed it, so Jazz answered 500 and the browser stayed signed out. Requests without a body no longer carry a JSON content type. `nodeHandler` serves an empty body a parser already read, and a JSON body `express.json()` parsed (linking a guest sends one) as that JSON. A Jazz request whose body another parser took in another form still fails, now with the code `request_body_already_read`, which the client reports instead of a bare server error.
