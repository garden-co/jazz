# Better Auth Integration Example

This example demonstrates how to integrate [Better Auth](https://www.better-auth.com/) with Jazz using Svelte.

## Getting started

To run this example, you may either:

- Clone the Jazz monorepo and run this example from within.
- Create a new Jazz project using this example as a template, and run that new project.

### Setting environment variables
- `BETTER_AUTH_URL`: The URL of the self-hosted Better Auth server
- `BETTER_AUTH_SECRET`: The encryption secret used by the self-hosted Better Auth server

### Using this example as a template

1. Create a new Jazz project, and use this example as a template.

```sh
npx create-jazz-app@latest betterauth-app --example betterauth-svelte
```

2. Navigate to the new project and install dependencies.

```sh
cd betterauth-app
pnpm install
```

3. Create a .env file (don't forget to set your [BETTER_AUTH_SECRET](https://www.better-auth.com/docs/installation#set-environment-variables)!)

```sh
mv .env.example .env
```

4. Start the development server

```sh
pnpm dev
```

https://www.better-auth.com/docs/installation#set-environment-variables

### Using the monorepo

This requires `pnpm` to be installed, see [https://pnpm.io/installation](https://pnpm.io/installation).

Clone the jazz repository.

```bash
git clone https://github.com/garden-co/jazz.git
```

Install and build dependencies.

```bash
pnpm i && npx turbo build
```

Go to the example directory.

```bash
cd jazz/examples/betterauth-svelte/
```

Create a .env file (don't forget to set your [BETTER_AUTH_SECRET](https://www.better-auth.com/docs/installation#set-environment-variables)!)

```sh
mv .env.example .env
```

Initialise the database.

```sh
sqlite3 sqlite.db < src/lib/better-auth_migrations/init.sql
```

Start the dev server.

```bash
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) with your browser to see the result.
