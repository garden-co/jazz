# Jazz React Native Development (Expo)

The majority of tools you need to develop a Jazz Expo application are imported from `jazz-tools/expo`.

## Core Setup: The Provider

The **`<JazzExpoProvider />`** is the essential entry point.

```tsx
import { JazzExpoProvider } from "jazz-tools/expo";

export default function App() {
  return (
    <JazzExpoProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <Main />
    </JazzExpoProvider>
  );
}
```

## Local Persistence

Jazz for Expo uses Expo's official modules for persistence:
- **Database**: `expo-sqlite`
- **Key-Value**: `expo-secure-store`

These are enabled by default.

### iOS Warning
On iOS, `expo-secure-store` persists credentials in the Keychain, which survives app uninstallation. If a user uninstalls and reinstalls the app, they may be stuck with credentials for a deleted locally-only account. To avoid this, either use `{when: "always"}` in the provider's sync options or use the following workaround:

```tsx
function RootLayout() {
  const [authSecretStorageKey, setAuthSecretStorageKey] = useState<
    string | null
  >(() => {
    const stored = Settings.get("jazz-authSecretStorageKey");
    if (stored) return stored;
    const newKey = "jazz-" + new Date();
    Settings.set({ "jazz-authSecretStorageKey": newKey });
    return newKey;
  });

  if (!authSecretStorageKey) {
    return null;
  }
  return (
    <JazzExpoProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        when: "never",
      }}
      authSecretStorageKey={authSecretStorageKey}
    >
      <App />
    </JazzExpoProvider>
  );
}
```

## Reactive Data Hooks

Jazz provides several hooks to consume and mutate CoValues reactively.

*   **`useCoState`**: Loads any CoValue by its ID.
*   **`useCoStates`**: Loads multiple CoValues by their IDs.
*   **`useAccount`**: Retrieves the current account.

## Authentication

*   **Passkey Auth**: Requires `react-native-passkey` and development build (not Expo Go).
    *   Install: `npm install react-native-passkey`
    *   Config: Associated Domains (iOS), Digital Asset Links (Android).
*   **Clerk**: Use `useClerk` from `@clerk/clerk-expo`:

```tsx
import { useClerk, ClerkProvider, ClerkLoaded } from "@clerk/clerk-expo";
import { resourceCache } from "@clerk/clerk-expo/resource-cache";
import { JazzExpoProviderWithClerk } from "jazz-tools/expo";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const clerk = useClerk();

  return (
    <JazzExpoProviderWithClerk
      clerk={clerk}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzExpoProviderWithClerk>
  );
}

export default function RootLayout() {
  const publishableKey = process.env.EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY;

  if (!publishableKey) {
    throw new Error(
      "Missing Publishable Key. Please set EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY in your .env",
    );
  }

  return (
    <ClerkProvider
      tokenCache={tokenCache}
      publishableKey={publishableKey}
      __experimental_resourceCache={resourceCache}
    >
      <ClerkLoaded>
        <JazzAndAuth>
          <MainScreen />
        </JazzAndAuth>
      </ClerkLoaded>
    </ClerkProvider>
  );
}
```

## Polyfills
Jazz provides a quick way to apply necessary polyfills. Import them in your root layout (e.g., `app/_layout.tsx`):

```tsx
import "jazz-tools/expo/polyfills";
```

## Image Handling

The **`<Image />`** component is the recommended way to display images.

### Installation
Jazz image creation depends on `expo-image-manipulator`. Ensure it is installed in your project.

### Usage
```tsx
import { Image } from "jazz-tools/expo";

// Standard usage
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

