# Jazz Durable Object Example

Simple example showing how to use Jazz with Cloudflare Durable Object with super
simple KV store.

## Setup

1. [Create a Server Worker](https://jazz.tools/docs/react/server-side/quickstart#create-a-server-worker)
2. fill in the `.dev.vars` file, check the `.dev.vars.example` for the keys
3. `pnpm install` and `pnpm run dev`

## API methods

`POST /` to get the durable object id for new namespace

`POST /{doId}/{key}` with plain text content to set value of `key`

`GET /{doId}/{key}` to get value of `key`
