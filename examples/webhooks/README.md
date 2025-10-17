# Webhook example

## Setup

1. Create a worker account (will be used to modify a CoValue and register a webhook):

    ```bash
    pnpm exec jazz-run account create --name "Webhook User" > .env.local
    ```

2. Create a registry:

    ```bash
    pnpm exec jazz-run webhook create-registry >> .env.local
    ```

    Your .env.local file should now have credentials for both the worker and the registry account, as well as a registry ID.

3. Allow the worker created in step 1 to register a webhook:

    ```bash
    set -a && source .env.local && pnpm exec jazz-run webhook grant --accountID $JAZZ_WORKER_ACCOUNT && set +a
    ```

4. Run the webhook registry:

    ```bash
    pnpm exec jazz-run webhook run
    ```

5. In a new terminal, run the target server:

    ```bash
    pnpm exec tsx target.ts
    ```

6. In a new terminal, run the source:

    ```bash
    set -a && source .env.local && pnpm exec tsx source.ts && set +a
    ```

After running the source, you should see something like the following output in the target server:

```
=== Incoming Request ===
Method: POST
URL: /
Headers: {
  "host": "localhost:4444",
  "connection": "keep-alive",
  "content-type": "application/json",
  "accept": "*/*",
  "accept-language": "*",
  "sec-fetch-mode": "cors",
  "user-agent": "node",
  "accept-encoding": "gzip, deflate",
  "content-length": "58"
}
Body: {"coValueId":"co_zVYhruFxFzorkWM9ao7CFg75H3o","updates":2}
========================

=== Incoming Request ===
Method: POST
URL: /
Headers: {
  "host": "localhost:4444",
  "connection": "keep-alive",
  "content-type": "application/json",
  "accept": "*/*",
  "accept-language": "*",
  "sec-fetch-mode": "cors",
  "user-agent": "node",
  "accept-encoding": "gzip, deflate",
  "content-length": "58"
}
Body: {"coValueId":"co_zVYhruFxFzorkWM9ao7CFg75H3o","updates":3}
========================
```