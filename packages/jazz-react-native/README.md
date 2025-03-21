# 🎷 Jazz + React Native

Jazz requires a [React Native development build](https://reactnative.dev/docs/getting-started-without-a-framework). Jazz also supports the [New Architecture](https://reactnative.dev/architecture/landing-page).

Tested with:

```json
"react-native": ">=0.74.5",
"react": "^18.2.0",
```

## 🚀 Setup

### Create a New Project

(skip this step if you already have one)

```bash
npx create-expo-app -e with-router-tailwind my-jazz-app
cd my-jazz-app
```

### Install dependencies

```bash
npm install @react-native-community/netinfo @bam.tech/react-native-image-resizer

npm i -S @azure/core-asynciterator-polyfill react-native-url-polyfill readable-stream react-native-get-random-values @craftzdog/react-native-buffer @op-engineering/op-sqlite

npm i -D @babel/plugin-transform-class-static-block

npm i -S jazz-tools jazz-react-native jazz-react-native-media-images
```

> note: Hermes has added support for `atob` and `btoa` in React Native 0.74.  If you are using earlier versions, you may also need to polyfill `atob` and `btoa` in your `package.json` . Packages to try include `text-encoding` and `base-64`, and you can drop `@bacons/text-decoder`.

### Install Pods

```bash
npx pod-install
```

### Configure Metro

#### Regular Repositories

If you are not working within a monorepo, create a new file metro.config.js in the root of your project with the following content:

```js
const { getDefaultConfig } = require("expo/metro-config");
const path = require("path");
const workspaceRoot = path.resolve(__dirname);
const config = getDefaultConfig(projectRoot);
config.resolver.sourceExts = ["mjs", "js", "json", "ts", "tsx"];
config.resolver.requireCycleIgnorePatterns = [/(^|\/|\\)node_modules($|\/|\\)/];
module.exports = config;
```

#### Monorepos

For monorepos, use the following metro.config.js:

```js
const { getDefaultConfig } = require("expo/metro-config");
const { FileStore } = require("metro-cache");
const path = require("path");

// eslint-disable-next-line no-undef
const projectRoot = __dirname;
const workspaceRoot = path.resolve(projectRoot, "../..");

const config = getDefaultConfig(projectRoot);

config.watchFolders = [workspaceRoot];
config.resolver.nodeModulesPaths = [
    path.resolve(projectRoot, "node_modules"),
    path.resolve(workspaceRoot, "node_modules"),
];
config.resolver.sourceExts = ["mjs", "js", "json", "ts", "tsx"];
config.resolver.requireCycleIgnorePatterns = [/(^|\/|\\)node_modules($|\/|\\)/];
config.cacheStores = [
    new FileStore({
        root: path.join(projectRoot, "node_modules", ".cache", "metro"),
    }),
];

module.exports = config;
```

### Additional Monorepo Configuration (for pnpm users)

- Add node-linker=hoisted to the root .npmrc (create this file if it doesn’t exist).
- Add the following to the root package.json:

```json
"pnpm": {
    "peerDependencyRules": {
        "ignoreMissing": [
            "@babel/*",
            "expo-modules-*",
            "typescript"
        ]
    }
}
```

For more information, refer to [this](https://github.com/byCedric/expo-monorepo-example#pnpm-workarounds) Expo monorepo example.

### Configure Babel

Add `@babel/plugin-transform-class-static-block` to the array of Babel plugins inside `babel.config.js`:

```js
module.exports = function (api) {
    api.cache(true);
    return {
        presets: ["babel-preset-expo"],
        plugins: [
            "nativewind/babel",
            "@babel/plugin-transform-class-static-block",
        ],
    };
};
```

### Add Polyfills

Create a file `polyfills.js` at the project root with the following content:

```js
import { polyfillGlobal } from 'react-native/Libraries/Utilities/PolyfillFunctions';

import { Buffer } from '@craftzdog/react-native-buffer';
polyfillGlobal('Buffer', () => Buffer);

import { ReadableStream } from 'readable-stream';
polyfillGlobal('ReadableStream', () => ReadableStream);

import '@azure/core-asynciterator-polyfill';

import '@bacons/text-decoder/install';

import 'react-native-get-random-values';
```

Update `index.js` based on whether you are using expo-router or not:

#### If using `expo-router`

```js
import "./polyfills";
import "expo-router/entry";
```

#### Without `expo-router`

```js
import "./polyfills";
import { registerRootComponent } from "expo";
import App from "./src/App";
registerRootComponent(App);
```

Lastly, ensure that the `"main"` field in your `package.json` points to `index.js`:

```js
"main": "index.js",
```

## 🎉 How to Use Jazz

### `createJazzRNApp()`

Create a file `jazz.tsx` with the following contents:

```js
import { createJazzRNApp } from "jazz-react-native";

export const Jazz = createJazzRNApp();
export const { useAccount, useCoState, useAcceptInvite } = Jazz;
```

You can optionally pass a custom `kvStore` and `AccountSchema` to `createJazzRNApp()`, otherwise, it defaults to `ExpoSecureStoreAdapter` and `Account`.

### Choosing an Auth Method

Refer to the Jazz + React Native demo projects for implementing authentication:

- [DemoAuth Example](https://github.com/garden-co/jazz/tree/main/examples/chat-rn-expo)
- [ClerkAuth Example](https://github.com/garden-co/jazz/tree/main/examples/chat-rn-expo-clerk)

In the demos, you'll find details on:

- Using JazzProvider with your chosen authentication method
- Defining a Jazz schema
- Creating and subscribing to covalues
- Handling invites

### 🖼️ Working with Images

To work with images in Jazz, import the `createImage` function from [`jazz-react-native-media-images`](https://github.com/garden-co/jazz/tree/main/packages/jazz-react-native-media-images).

```js
import { createImage } from "jazz-react-native-media-images";

const base64ImageDataURI = "data:image/png;base64,...";

const image = await createImage(base64ImageDataURI, {
    owner: newPetPost._owner,
    maxSize: 2048, // optional: specify maximum image size
});

someCovalue.image = image;
```

For a complete implementation, please refer to [this](https://github.com/garden-co/jazz/blob/main/examples/pets/src/3_NewPetPostForm.tsx) demo.

### 📱 Running Your App

```bash
npx expo run:ios
npx expo run:android
```
