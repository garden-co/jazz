# Better Auth Integration Example

This example demonstrates using Jazz with Better Auth and Next.js.

## Getting started

To run this example, you may either:
* Clone the Jazz monorepo and run this example from within.
* Create a new Jazz project using this example as a template, and run that new project.

### Setting environment variables
- `NEXT_PUBLIC_AUTH_BASE_URL`: A URL to a Better Auth server. If undefined, the example will self-host a Better Auth server.
- `BETTER_AUTH_SECRET`: The encryption secret used by the self-hosted Better Auth server (required only if `NEXT_PUBLIC_AUTH_BASE_URL` is undefined)
- `GITHUB_CLIENT_ID`: The client ID for the GitHub OAuth provider used by the self-hosted Better Auth server (required only if `NEXT_PUBLIC_AUTH_BASE_URL` is undefined)
- `GITHUB_CLIENT_SECRET`: The client secret for the GitHub OAuth provider used by the self-hosted Better Auth server (required only if `NEXT_PUBLIC_AUTH_BASE_URL` is undefined)

### Using this example as a template

1. Create a new Jazz project, and use this example as a template.
```sh
npx create-jazz-app@latest betterauth-app --example betterauth
```
2. Navigate to the new project and start the development server.
```sh
cd betterauth-app
pnpm install
pnpm dev
```

### Using the monorepo

This requires `pnpm` to be installed; see [https://pnpm.io/installation](https://pnpm.io/installation).

1. Clone the `jazz` repository.
```sh
git clone https://github.com/garden-co/jazz.git
```
2. Install dependencies.
```sh
cd jazz
pnpm install
```
3. Navigate to the example and start the development server.
```sh
cd examples/betterauth
pnpm dev
```

The example should be running at [http://localhost:3000](http://localhost:3000) by default.
