# Native Expo setup

Use the installed working Expo example as the primary template. Package and Expo peer versions must
match the project rather than a remembered latest release.

## Dependencies and app configuration

Keep these as direct application dependencies:

- `jazz-tools`
- `jazz-rn`
- `expo-crypto`
- `expo-secure-store`
- `metro-runtime`

Use the project's package manager and `expo install` for Expo-owned packages. The native Jazz module
requires a development build and React Native codegen; Expo Go cannot load it.

The current repository example enables Hermes and New Architecture:

```json
{
  "expo": {
    "jsEngine": "hermes",
    "newArchEnabled": true
  }
}
```

Confirm those requirements against the installed `jazz-rn` package before changing an established
application.

## Entry point and imports

Load polyfills first:

```js
import "jazz-tools/expo/polyfills";
import registerRootComponent from "expo/src/launch/registerRootComponent";
import App from "./App";

registerRootComponent(App);
```

The polyfill establishes web-compatible globals needed by the Jazz runtime before Jazz modules are
evaluated.

Use the package split deliberately:

```tsx
import { JazzProvider, useAll, useDb, useSession } from "jazz-tools/react-native";
import { ExpoAuthSecretStore, useLocalFirstAuth } from "jazz-tools/expo";
```

Do not import browser React bindings into the native app.

## Metro development integration

An ESM Metro config can initialize Jazz's development helper:

```js
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { withJazz } from "jazz-tools/dev/expo";

const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");
const projectRoot = path.dirname(fileURLToPath(import.meta.url));
const config = getDefaultConfig(projectRoot);

config.resolver.unstable_enableSymlinks = true; // when required by the pnpm workspace
await withJazz({}, { schemaDir: projectRoot });

export default config;
```

The helper starts or connects to the configured development server and injects public app/server
values. It does not authorize putting admin or backend secrets in the mobile environment.

Follow the project's existing Babel setup. Add an `import.meta` transform only when the installed
Expo/Jazz combination requires it and a real native build demonstrates the failure.

## Provider setup

Resolve identity before constructing a stable config:

```tsx
export default function App() {
  const secret = React.use(ExpoAuthSecretStore.getOrCreateSecret());
  const config = React.useMemo(
    () => ({
      appId,
      serverUrl,
      env: "dev" as const,
      userBranch: "main",
      secret,
      dataPath,
    }),
    [secret, dataPath],
  );

  return <JazzProvider config={config}>{/* application */}</JazzProvider>;
}
```

`useLocalFirstAuth()` is preferable when the UI also needs sign-out or recovery lifecycle helpers.
Do not recreate the config or secret on every render.

React Native uses the normal React query shape. Build queries from current React state and pass
`undefined` to skip. Preserve `undefined` as first-result loading and `[]` as a completed empty
query. Do not copy Jazz rows into component state merely to make them reactive.

## Native verification

Run the project's equivalents of:

```bash
pnpm exec expo prebuild --platform ios --clean
pnpm exec expo prebuild --platform android --clean
pnpm exec expo run:ios
pnpm exec expo run:android
```

Confirm the generated native projects contain and link the `jazz-rn` module. A JavaScript-only
typecheck or Metro start does not verify this.
