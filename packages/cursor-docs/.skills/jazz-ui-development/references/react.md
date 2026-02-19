# Jazz React Development

The majority of tools you need to develop a Jazz React application are imported from `jazz-tools/react`.

## Core Setup: The Provider

The **`<JazzReactProvider />`** is the essential entry point that connects a React application to the Jazz network, managing data synchronisation, local storage, and authentication context.

*   **`sync` property**: Configures the connection to Jazz Cloud or a self-hosted peer, including **`when`** to sync (e.g., `"always"`, `"signedUp"`, or `"never"`).
*   **`AccountSchema`**: Required to define the structure of the user's account and trigger migrations upon login.
*   **`onAnonymousAccountDiscarded`**: Use this handler to migrate data from a temporary local account to a newly authenticated account.
*   **SSR Support**: Use the **`enableSSR`** prop when building with frameworks like Next.js to allow server-rendered pages to access public data.

## Reactive Data Hooks

Jazz provides several hooks to consume and mutate CoValues reactively. These hooks automatically re-render components when the underlying data changes.

### Standard Hooks
*   **`useCoState`**: Loads any CoValue by its ID. It is highly effective when combined with a **`select`** function to filter or sort data (e.g., for semantic search).
*   **`useCoStates`**: Similar to `useCoState`, but loads multiple CoValues by their IDs. Helpful for fetching multiple CoValues dynamically without violating the React Rules of Hooks or creating separate components for each CoValue.
*   **`useAccount`**: Retrieves the current account based on the provided schema. It accepts a **`resolve`** query to deeply load nested data in a single request.

### Suspense Hooks
For an improved developer experience with **React Suspense**, use the suspense-compatible versions:
*   **`useSuspenseCoState`**: Similar to `useCoState`, but integrates with Suspense boundaries for loading states.
*   **`useSuspenseCoStates`**: Similar to `useCoStates`, but integrates with Suspense boundaries for loading states.
*   **`useSuspenseAccount`**: Throws a promise until the account and specified `resolve` query are ready.

#### Error Handling
*   **`getJazzErrorType`**: Within a standard React **`ErrorBoundary`**, use this helper to detect specific Jazz failures like `"unauthorized"`, `"deleted"`, or `"unavailable"` to render appropriate fallbacks.

### Options

All six reactive data hooks take an options object with the following properties:
*   **`resolve`**: A query function that resolves the data.
*   **`select`**: A selector function to extract the desired data from the resolved value. **Avoid expensive computations directly within selectors**, as they run every time the CoValue updates. Instead, extract heavy logic into a separate **`useMemo`** call.
*   **`equalityFn`**: An optional function to compare previous and new data for equality. If the equality function returns `true`, the hook will skip updating the state (**note that the selector will still run**).
*   **`unstable_branch`**: An optional function to use Jazz's internal branching/versioning logic.
*   **`preloaded`**: A serialized string of preloaded data for the initial render (normally used when the component is server-side rendered to provide a pre-hydration state).

### Resolving Data

## Other Hooks

*   **`usePasskeyAuth`**: Used for building custom authentication components. It returns the current auth state and methods like **`signUp`** and **`logIn`**.
*   **`usePassphraseAuth`**: Facilitates login using a wordlist-based passphrase. Can be used for recovery, as an alternative login for users without passkey support, or on its own.
*   **`useIsAuthenticated`**: A simple boolean hook to check if the user is currently logged in.
*   **`useAgent`**: Returns the current agent, primarily for detecting guests (`const isGuest = agent.$type$ !== "Account";`).
*   **`useLogOut`**: Returns a function to log out the current user.
*   **`useSyncConnectionStatus`**: Returns `true` if connected to the sync server, `false` otherwise.

## Specialized UI Components

### Image Handling
The **`<Image />`** component is the recommended way to display images in React.
*   It handles **progressive loading**, automatically selecting the best resolution for the current viewport.
*   It supports **lazy loading** via the native browser loading attribute.
*   It automatically renders **blurry placeholders** if they were generated during image creation.

```tsx
<Image imageId={image.$jazz.id} alt="Profile" width={600} />
```

The component's props are:
```ts
export type ImageProps = Omit<
  HTMLImgAttributes,
  "src" | "srcset" | "width" | "height"
> & {
  imageId: string;
  width?: number | "original";
  height?: number | "original";
  placeholder?: string;
  loading?: "lazy" | "eager";
};
```

```tsx
// Image with the highest resolution available
<Image imageId="123" />

// Image with width 1920 and height 1080
<Image imageId="123" width="original" height="original" />

// Keeps the aspect ratio (height: 338)
<Image imageId="123" width={600} height="original" />

// As above, aspect ratio is maintained (width: 1067)
<Image imageId="123" width="original" height={600} />

// Renders as a 600x600 square
<Image imageId="123" width={600} height={600} />

// Lazy load the image
<Image imageId="123" loading="lazy" />
```

Create images using the `createImage` function imported from `jazz-tools/media`.

```ts
declare function createImage(
  image: Blob | File | string,
  options?: {
    owner?: Group;
    placeholder?: false | "blur";
    maxSize?: number;
    progressive?: boolean;
  },
): Promise<Loaded<typeof ImageDefinition, { original: true }>>;
```

### Authentication
*   **`PasskeyAuthBasicUI`**: A ready-to-use component that provides a basic interface for passkey registration and login. Useful for prototyping, but use the `usePasskeyAuth` hook to build a more custom interface for anything but the most trivial use cases.

#### Authentication with BetterAuth

**1. Client (`src/lib/auth-client.ts`)**
```ts
import { createAuthClient } from "better-auth/client";
import { jazzPluginClient } from "jazz-tools/better-auth/auth/client";

export const betterAuthClient = createAuthClient({
  plugins: [jazzPluginClient()],
});
```

**2. Provider (`src/components/JazzRoot.tsx`)**
```tsx
import { AuthProvider } from "jazz-tools/better-auth/auth/react";
import { JazzReactProvider } from "jazz-tools/react";
import { betterAuthClient } from "@/lib/auth-client";

export function JazzRoot({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider sync={{ peer: "wss://cloud.jazz.tools/?key=..." }}>
      <AuthProvider betterAuthClient={betterAuthClient}>
        {children}
      </AuthProvider>
    </JazzReactProvider>
  );
}
```
Refer to <https://github.com/garden-co/jazz/tree/main/examples/betterauth> for a complete reference implementation.

#### Authentication with Clerk
**`JazzReactProviderWithClerk`**: A specialized provider that integrates with Clerk authentication. Usage:
```tsx
import { useClerk, ClerkProvider } from "@clerk/clerk-react";
import { JazzReactProviderWithClerk } from "jazz-tools/react";

function JazzProvider({ children }: { children: React.ReactNode }) {
  const clerk = useClerk();

  return (
    <JazzReactProviderWithClerk
      clerk={clerk}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzReactProviderWithClerk>
  );
}
```
## Collaboration & Invites

Managing shared data in React is simplified through dedicated hooks for the invite flow.

*   **`createInviteLink`**: Generates a shareable URL with a fragment identifier (hash) to keep the invite secret out of server logs.
*   **`useAcceptInvite`**: A client-side hook that listens for invite secrets in the URL and executes a callback (**`onAccept`**) once the user successfully joins the group.

## Advanced Patterns

### Form Handling
*   **Updates**: Mutate CoValues directly in event handlers (e.g., `onChange`), and Jazz will sync the changes instantly. Be sure to use the correct mutation methods (`$jazz.set`, `$jazz.push`, etc.).
*   **Creation**: Use **`.partial()`** schemas to build up data incrementally in a form before pushing the final object to a list.
*   **Save Buttons**: If you require a "Save" or "Cancel" workflow, use **`unstable_branch`** within `useCoState` to isolate changes in a private branch until the user explicitly merges them.

### SSR Hydration
To prevent "flicker" on server-rendered pages, use **`$jazz.export()`** to serialise data on the server and pass it to the **`preloaded`** option of **`useCoState`** on the client.

### Accessing Context
*   **`useJazzContext`**: Returns the `JazzContextManager`. Throws if used outside the provider.
*   **`useJazzContextValue`**: Returns the current value of the `JazzContext`. Throws if the context is not initialized.

### Developer Tools
*   **`<JazzInspector />`**: Embeds a button to launch the Jazz Inspector, allowing developers to visually audit CoValues and account credentials directly in the app.

**ONLY LOAD THIS DOCUMENTATION IF ABSOLUTELY NECESSARY. THIS IS A VERY LARGE FILE, ~120 000 TOKENS**: the full documentation is available for React here: <https://jazz.tools/react/llms-full.txt> 
