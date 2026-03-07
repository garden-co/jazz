# Jazz Inspector

Inspector UI for exploring Jazz databases, usable either as a standalone web app or as a Chrome DevTools panel.

## Running the Inspector in standalone mode

You can run the Inspector as a regular web app that connects to a Jazz cloud server.

```sh
cd /Users/takeno/Workspace/Jazz/jazz2/packages/inspector
pnpm dev
```

Then open `http://localhost:5173` in your browser (Vite’s default dev server port).

- **First-time configuration**
  - **serverUrl**: base URL of your Jazz cloud server (for example `https://api.my-jazz-cloud.com`).
  - **appId**: the Jazz app identifier you want to inspect.
  - **adminSecret**: admin secret for that app.
  - **env**: environment name (for example `dev`, `staging`, `prod`). Defaults to `dev` if left empty.
  - **branch**: logical branch name (defaults to `main`).
  - **serverPathPrefix** (optional): path prefix if your server is not mounted at `/`.

## Building the DevTools extension

The Chrome DevTools extension bundle is produced via Vite’s `extension` mode.

- **Build only the extension from the package**

```sh
cd packages/inspector
pnpm build:extension
```

## Installing the DevTools extension in Chrome

1. **Build the extension** (see above) so that `dist-extension/` exists.
2. Open Chrome and go to `chrome://extensions`.
3. Enable **Developer mode** (toggle in the top-right).
4. Click **Load unpacked** and select the `packages/inspector/dist-extension` directory.
5. Open DevTools on any page that runs a Jazz app; a **Jazz Inspector** panel should appear in the DevTools tab strip.

If the panel is open but no runtime is connected yet, the Inspector will show a waiting state until a Jazz runtime with devtools support is active in the page.
