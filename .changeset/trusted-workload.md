---
"jazz-tools": patch
---

jazz-tools server: `--trusted-workload-issuer/-audience/-subject` makes a server trust one OIDC workload (for example a Vercel function) instead of shared secrets. Every request and WebSocket upgrade must carry its token in `X-Jazz-Backend-Credential`; the backend states its access (`backend` or `admin`) in `X-Jazz-Backend-Access`, and secrets a request or prelude carries itself are ignored.
