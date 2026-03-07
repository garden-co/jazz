# Jazz Svelte Development

The majority of tools you need to develop a Jazz Svelte application are imported from `jazz-tools/svelte`.
Jazz Svelte integration is built on top of **Svelte 5 Runes**.

## Core Setup: The Provider

The **`<JazzSvelteProvider />`** is the essential entry point that connects a Svelte application to the Jazz network, managing data synchronisation, local storage, and authentication context.

*   **`sync` property**: Configures the connection to Jazz Cloud or a self-hosted peer, including **`when`** to sync (e.g., `"always"`, `"signedUp"`, or `"never"`).
*   **`AccountSchema`**: Required to define the structure of the user's account and trigger migrations upon login.
*   **`onAnonymousAccountDiscarded`**: Use this handler to migrate data from a temporary local account to a newly authenticated account.
*   **SSR Support**: Use the **`enableSSR`** prop (`true`/`false`) when building with SvelteKit to allow server-rendered pages to access public data.

## Reactive Data Classes (Runes)

Jazz provides reactive classes to consume and mutate CoValues. These classes use Svelte 5 runes and automatically trigger updates when the underlying data changes.

*   **`CoState`**: Loads any CoValue by its ID.
    ```typescript
    const state = new CoState(MySchema, () => id, options);
    // Access data via state.current
    ```
*   **`AccountCoState`**: Loads the current account based on the provided schema.
    ```typescript
    const account = new AccountCoState(MyAccountSchema, options);
    // Access data via account.current
    ```

### Options

Both classes take an options object (or a function returning one) with the following properties:

*   **`resolve`**: A query function that resolves the data.
*   **`unstable_branch`**: An optional function to use Jazz's internal branching/versioning logic.

## Other Helpers

*   **`useIsAuthenticated`**: A hook to check if the user is currently logged in.
    ```typescript
    const auth = useIsAuthenticated();
    $effect(() => {
        console.log(auth.current);
    });
    ```
*   **`SyncConnectionStatus`**: A class that provides the current connection status.
    ```typescript
    const status = new SyncConnectionStatus();
    // Access status.current
    ```
*   **`InviteListener`**: A class that listens for invite secrets in the URL and executes a callback (`onAccept`).

## Specialized UI Components

### Image Handling

The **`<Image />`** component is the recommended way to display images in Svelte.
*   It handles **progressive loading**, automatically selecting the best resolution for the current viewport.
*   It supports **lazy loading** via the native browser loading attribute.
*   It automatically renders **blurry placeholders** if they were generated during image creation.

```svelte
<Image
  imageId={image.$jazz.id}
  alt="Profile"
  class="h-auto max-h-[20rem] max-w-full rounded-t-xl mb-1"
/>
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

```svelte
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
    maxSize?: number; // longest side
    progressive?: boolean;
  },
): Promise<Loaded<typeof ImageDefinition, { original: true }>>;
```

### Authentication

#### Authentication with BetterAuth

**1. Client (`src/lib/auth-client.ts`)**
```ts
import { createAuthClient } from "better-auth/client";
import { jazzPluginClient } from "jazz-tools/better-auth/auth/client";

export const betterAuthClient = createAuthClient({
  plugins: [jazzPluginClient()],
});
```

**2. Provider (`src/routes/+layout.svelte`)**
```svelte
<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { betterAuthClient } from "$lib/auth-client"; // Your configured client
  import AuthProvider from "jazz-tools/better-auth/auth/svelte";

  // ... props definition
</script>

<JazzSvelteProvider {sync} enableSSR>
  <AuthProvider {betterAuthClient} />
  {@render children?.()}
</JazzSvelteProvider>
```
Refer to <https://github.com/garden-co/jazz/tree/main/examples/betterauth-svelte> for a complete reference implementation.

#### Authentication with Clerk
Be sure to install `svelte-clerk` first: `npm install svelte-clerk`
*   **`<JazzSvelteProviderWithClerk />`**: A specialized provider that integrates with Clerk authentication.

**1. Wrapper (`src/lib/components/JazzClerkWrapper.svelte`)**

```svelte
<script lang="ts"> 
import { useClerkContext } from "svelte-clerk"; 
import { JazzSvelteProviderWithClerk } from "jazz-tools/svelte";
const apiKey = "you@example.com"; 
const ctx = useClerkContext(); 
const clerk = $derived(ctx.clerk); 
let { children } = $props(); 
</script>
<JazzSvelteProviderWithClerk 
  {clerk} 
  sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
>
  {#snippet fallback()}
    <p>Loading...</p>
  {/snippet}
  {@render children?.()}
</JazzSvelteProviderWithClerk>
```

**2. Layout (`src/routes/+layout.svelte`)**

```svelte
<script lang="ts">
  import { ClerkProvider } from "svelte-clerk";
  import { PUBLIC_CLERK_PUBLISHABLE_KEY } from "$env/static/public";
  import JazzClerkWrapper from "$lib/components/JazzClerkWrapper.svelte";

  let { children } = $props();
</script>

<ClerkProvider publishableKey={PUBLIC_CLERK_PUBLISHABLE_KEY}>
  <JazzClerkWrapper>
    {@render children?.()}
  </JazzClerkWrapper>
</ClerkProvider>
```

### Accessing Context

*   **`getJazzContext()`**: Returns the Jazz context. Must be used within a component or function called during component initialization.
*   **`getAuthSecretStorage()`**: Returns the auth secret storage.

### Developer Tools
**Jazz Inspector**

```svelte
<script lang="ts">
  import "jazz-tools/inspector/register-custom-element"
</script>

<jazz-inspector></jazz-inspector>
```

**ONLY LOAD THIS DOCUMENTATION IF ABSOLUTELY NECESSARY. THIS IS A VERY LARGE FILE, ~80 000 TOKENS**: the full documentation is available for Svelte here: <https://jazz.tools/svelte/llms-full.txt>
