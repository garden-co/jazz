# Jazz React Native Development (CLI)

The majority of tools you need to develop a Jazz React Native application (without Expo) are imported from `jazz-tools/react-native`.

## Core Setup: The Provider

The **`<JazzReactNativeProvider />`** is the essential entry point.

```tsx
import { JazzReactNativeProvider } from "jazz-tools/react-native";

export default function App() {
  return (
    <JazzReactNativeProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=your-project-secret@garden.co",
      }}
    >
      <Main />
    </JazzReactNativeProvider>
  );
}
```

*   **`sync` property**: Configures the connection to Jazz Cloud or a self-hosted peer.
*   **`AccountSchema`**: Required to define the structure of the user's account.

## Reactive Data Hooks

Jazz provides several hooks to consume and mutate CoValues reactively.

*   **`useCoState`**: Loads any CoValue by its ID.
*   **`useCoStates`**: Loads multiple CoValues by their IDs.
*   **`useAccount`**: Retrieves the current account.

### Authentication

*   **Passkey Auth**: Requires `react-native-passkey`.
    1.  Install: `npm install react-native-passkey`
    2.  Config: Add Associated Domains (iOS) and Digital Asset Links (Android).
    3.  See [Passkey Documentation](https://jazz.tools/docs/react-native/key-features/authentication/passkey.md) for full setup.

## Polyfills
Jazz provides a quick way to apply necessary polyfills. Import them in your root entry file (e.g., `index.js`):

```js
import "jazz-tools/react-native/polyfills";
```

## Image Handling

The **`<Image />`** component is the recommended way to display images.

### Installation
Jazz image creation depends on `@bam.tech/react-native-image-resizer`. Ensure it is installed in your project.

### Usage
```tsx
import { Image } from "jazz-tools/react-native";

<Image 
  imageId={user.profile.picture.$jazz.id} 
  width={100} 
  height={100} 
/>
```

### Props
*   **`imageId`**: The ID of the image CoValue.
*   **`width` / `height`**: Number or `"original"`. Determines the resolution to load.
*   **`placeholder`**: Optional custom placeholder.

