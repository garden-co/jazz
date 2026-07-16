# Jazz Inspector

Inspector UI for exploring Jazz databases, usable either as a standalone web app or as an embedded development overlay.

## Running the Inspector in standalone mode

You can run the Inspector as a regular web app that connects to a Jazz sync server.

```sh
cd packages/inspector
pnpm dev
```

Then open `http://localhost:5173` in your browser (Vite’s default dev server port).

- **First-time configuration**
  - **serverUrl**: base URL of your Jazz cloud server (for example `https://api.my-jazz-cloud.com`).
  - **appId**: the Jazz app identifier you want to inspect.
  - **adminSecret**: admin secret for that app.
  - **env**: environment name (for example `dev`, `staging`, `prod`). Defaults to `dev` if left empty.
  - **branch**: logical branch name (defaults to `main`).

The inspector derives app-scoped endpoints automatically from `serverUrl` and `appId`, so there is
no separate path-prefix setting.

## Building the Inspector

The package provides standalone web and embedded builds.

- **Build the standalone web app**

```sh
cd packages/inspector
pnpm build
```

- **Build the embedded inspector**

```sh
cd packages/inspector
pnpm build:embedded
```

The Jazz Vite and SvelteKit development integrations serve the embedded inspector as an in-app
overlay by default. Set their `inspector` option to `false` to disable it.
