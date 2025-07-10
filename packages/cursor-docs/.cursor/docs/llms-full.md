# Jazz

## Documentation

### Getting started

#### Introduction

# Learn some <span className="sr-only">Jazz</span> <JazzLogo className="h-[41px] -ml-0.5 -mt-[3px] inline" />

Welcome to the Jazz documentation!

**Note:** We just released [Jazz 0.14.0](/docs/upgrade/0-14-0) with a bunch of breaking changes and are still cleaning the docs up - see the [upgrade guide](/docs/upgrade/0-14-0) for details.

## Quickstart

You can use [`create-jazz-app`](/docs/tools/create-jazz-app) to create a new Jazz project from one of our starter templates or example apps:

<CodeGroup>
```sh
npx create-jazz-app@latest --api-key you@example.com
```
</CodeGroup>

Or set up Jazz yourself, using the following instructions for your framework of choice:

- [React](/docs/react/project-setup)
- [Next.js](/docs/react/project-setup#nextjs)
- [React Native](/docs/react-native/project-setup)
- [React Native Expo](/docs/react-native-expo/project-setup)
- [Vue](/docs/vue/project-setup)
- [Svelte](/docs/svelte/project-setup)

{/* <ContentByFramework framework="react">
  Or you can follow this [React step-by-step guide](/docs/react/guide) where we walk you through building an issue tracker app.
</ContentByFramework> */}

## Example apps

You can also find [example apps](/examples) with code most similar to what you want to build. These apps
make use of different features such as auth, file upload, and more.

## Sync and storage

Sync and persist your data by setting up a [sync and storage infrastructure](/docs/sync-and-storage) using Jazz Cloud, or do it yourself.

## Collaborative values

Learn how to structure your data using [collaborative values](/docs/schemas/covalues).

## LLM Docs

Get better results with AI by [importing the Jazz docs](/docs/ai-tools) into your context window.

## Get support

If you have any questions or need assistance, please don't hesitate to reach out to us on [Discord](https://discord.gg/utDMjHYg42).
We would love to help you get started.



#### Example apps



#### FAQs

# Installation and Setup

Add Jazz to your React application in minutes. This setup covers standard React apps, Next.js, and gives an overview of experimental SSR approaches.

Integrating Jazz with React is straightforward. You'll define data schemas that describe your application's structure, then wrap your app with a provider that handles sync and storage. The whole process takes just three steps:

1. [Install dependencies](#install-dependencies)
2. [Write your schema](#write-your-schema)
3. [Wrap your app in `<JazzProvider />`](#standard-react-setup)

Looking for complete examples? Check out our [example applications](/examples) for chat apps, collaborative editors, and more.

## Install dependencies

First, install the required packages:

<CodeGroup>
```bash
pnpm install jazz-react jazz-tools
```
</CodeGroup>

## Write your schema

Define your data schema using [CoValues](/docs/schemas/covalues) from `jazz-tools`.

<CodeGroup>
```tsx twoslash
// schema.ts

export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
});

export const AccountRoot = co.map({
  todos: co.list(TodoItem),
});

export const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});
```
</CodeGroup>

See [CoValues](/docs/schemas/covalues) for more information on how to define your schema.

## Standard React Setup

Wrap your application with `<JazzProvider />` to connect to the Jazz network and define your data schema:

<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
});

export const AccountRoot = co.map({
  todos: co.list(TodoItem),
});

export const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});
// @filename: app.tsx

function App() {
    return <div>Hello, world!</div>;
}
// ---cut---
// app.tsx

createRoot(document.getElementById("root")!).render(
  <JazzProvider
    sync={{ peer: "wss://cloud.jazz.tools/?key=you@example.com" }}
    AccountSchema={MyAppAccount}
  >
    <App />
  </JazzProvider>
);
```
</CodeGroup>

This setup handles:
- Connection to the Jazz sync server
- Schema registration for type-safe data handling
- Local storage configuration

With this in place, you're ready to start using Jazz hooks in your components. [Learn how to access and update your data](/docs/using-covalues/subscription-and-loading#subscription-hooks).

## Next.js Integration

### Client-side Only (Easiest)

The simplest approach for Next.js is client-side only integration:

<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
});

export const AccountRoot = co.map({
  todos: co.list(TodoItem),
});

export const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});
// @filename: app.tsx
// ---cut---
// app.tsx
"use client" // Mark as client component


export function JazzWrapper({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=you@example.com" }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzProvider>
  );
}
```
</CodeGroup>

Remember to mark any component that uses Jazz hooks with `"use client"`:

<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
});

export const AccountRoot = co.map({
  todos: co.list(TodoItem),
});

export const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});
// @filename: Profile.tsx

// ---cut---
// Profile.tsx
"use client"; // [!code ++]


export function Profile() {
  const { me } = useAccount(MyAppAccount, { resolve: { profile: true } });

  return <div>Hello, {me?.profile.name}</div>;
}
```
</CodeGroup>

### SSR Support (Experimental)

For server-side rendering, Jazz offers experimental approaches:

- Pure SSR
- Hybrid SSR + Client Hydration

#### Pure SSR

Use Jazz in server components by directly loading data with `CoValue.load()`.

{/*
<CodeGroup>
```tsx twoslash
// @errors: 18047
// @filename: schema.ts

export class MyItem extends CoMap {
  title = co.string;
}

export class MyCollection extends CoList.Of(co.ref(MyItem)) {}
// @filename: PublicData.tsx
const collectionID = "co_z123" as ID<MyCollection>;
// ---cut---
// Server Component (no "use client" directive)

export default async function PublicData() {
  // Load data directly in the server component
  const items = await MyCollection.load(collectionID);

  if (!items) {
    return <div>Loading...</div>;
  }

  return (
    <ul>
      {items.map(item => (
        item ? <li key={item.id}>{item.title}</li> : null
      ))}
    </ul>
  );
}
```
</CodeGroup>
*/}

This works well for public data accessible to the server account.

#### Hybrid SSR + Client Hydration

For more complex cases, you can pre-render on the server and hydrate on the client:

1. Create a shared rendering component.

{/*
<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export class MyItem extends CoMap {
  title = co.string;
}
// @filename: ItemList.tsx
// ---cut---
// ItemList.tsx - works in both server and client contexts
export function ItemList({ items }: { items: MyItem[] }) {
  return (
    <ul>
      {items.map(item => (
        <li key={item.id}>{item.title}</li>
      ))}
    </ul>
  );
}
```
</CodeGroup>
*/}

2. Create a client hydration component.

{/*
<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export class MyItem extends CoMap {
  title = co.string;
}
export class MyCollection extends CoList.Of(co.ref(MyItem)) {}
// @filename: ItemList.tsx

export function ItemList({ items }: { items: MyItem[] }) {
  return (
    <ul>
      {items.map(item => (
        <li key={item.id}>{item.title}</li>
      ))}
    </ul>
  );
}
// @filename: ItemListHydrator.tsx
// ItemListHydrator.tsx
const myCollectionID = "co_z123" as ID<MyCollection>;
// ---cut---
"use client"

export function ItemListHydrator({ initialItems }: { initialItems: MyItem[] }) {
  // Hydrate with real-time data once client loads
  const myCollection = useCoState(MyCollection, myCollectionID);

  // Filter out nulls for type safety
  const items = Array.from(myCollection?.values() || []).filter(
    (item): item is MyItem => !!item
  );

  // Use server data until client data is available
  const displayItems = items || initialItems;

  return <ItemList items={displayItems} />;
}
```
</CodeGroup>
*/}

3. Create a server component that pre-loads data.

{/*
<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export class MyItem extends CoMap {
  title = co.string;
}

export class MyCollection extends CoList.Of(co.ref(MyItem)) {}
// @filename: ItemList.tsx

export function ItemList({ items }: { items: MyItem[] }) {
  return (
    <ul>
      {items.map(item => (
        <li key={item.id}>{item.title}</li>
      ))}
    </ul>
  );
}
// @filename: ItemListHydrator.tsx
// ItemListHydrator.tsx
const myCollectionID = "co_z123" as ID<MyCollection>;
// ---cut---
"use client"

export function ItemListHydrator({ initialItems }: { initialItems: MyItem[] }) {
  // Hydrate with real-time data once client loads
  const myCollection = useCoState(MyCollection, myCollectionID);

  // Filter out nulls for type safety
  const items = Array.from(myCollection?.values() || []).filter(
    (item): item is MyItem => !!item
  );

  // Use server data until client data is available
  const displayItems = items || initialItems;

  return <ItemList items={displayItems} />;
}
// @filename: ServerItemPage.tsx
const myCollectionID = "co_z123" as ID<MyCollection>;
// ---cut---
// ServerItemPage.tsx
export default async function ServerItemPage() {
  // Pre-load data on the server
  const initialItems = await MyCollection.load(myCollectionID);

  // Filter out nulls for type safety
  const items = Array.from(initialItems?.values() || []).filter(
    (item): item is MyItem => !!item
  );

  // Pass to client hydrator
  return <ItemListHydrator initialItems={items} />;
}
```
</CodeGroup>
*/}

This approach gives you the best of both worlds: fast initial loading with server rendering, plus real-time updates on the client.

## Further Reading

- [Schemas](/docs/schemas/covalues) - Learn about defining your data model
- [Provider Configuration](/docs/project-setup/providers) - Learn about other configuration options for Providers
- [Authentication](/docs/authentication/overview) - Set up user authentication
- [Sync and Storage](/docs/sync-and-storage) - Learn about data persistence

---

### server-side Implementation

# Node.JS / server workers

The main detail to understand when using Jazz server-side is that Server Workers have Jazz `Accounts`, just like normal users do.

This lets you share CoValues with Server Workers, having precise access control by adding the Worker to `Groups` with specific roles just like you would with other users.

[See the full example here.](https://github.com/garden-co/jazz/tree/main/examples/jazz-paper-scissors)

## Generating credentials

Server Workers typically have static credentials, consisting of a public Account ID and a private Account Secret.

To generate new credentials for a Server Worker, you can run:

<CodeGroup>
```sh
npx jazz-run account create --name "My Server Worker"
```
</CodeGroup>

The name will be put in the public profile of the Server Worker's `Account`, which can be helpful when inspecting metadata of CoValue edits that the Server Worker has done.

## Storing & providing credentials

Server Worker credentials are typically stored and provided as environmental variables.

**Take extra care with the Account Secret &mdash; handle it like any other secret environment variable such as a DB password.**

## Starting a server worker

You can use `startWorker` from `jazz-nodejs` to start a Server Worker. Similarly to setting up a client-side Jazz context, it:

- takes a custom `AccountSchema` if you have one (for example, because the worker needs to store information in it's private account root)
- takes a URL for a sync & storage server

`startWorker` expects credentials in the `JAZZ_WORKER_ACCOUNT` and `JAZZ_WORKER_SECRET` environment variables by default (as printed by `npx account create ...`), but you can also pass them manually as `accountID` and `accountSecret` parameters if you get them from elsewhere.

<CodeGroup>
```ts twoslash
class MyWorkerAccount extends Account {}
// ---cut---

const { worker } = await startWorker({
  AccountSchema: MyWorkerAccount,
  syncServer: 'wss://cloud.jazz.tools/?key=you@example.com',
});
```
</CodeGroup>

`worker` acts like `me` (as returned by `useAccount` on the client) - you can use it to:

- load/subscribe to CoValues: `MyCoValue.subscribe(id, worker, {...})`
- create CoValues & Groups `const val = MyCoValue.create({...}, { owner: worker })`

## Using CoValues instead of requests

Just like traditional backend functions, you can use Server Workers to do useful stuff (computations, calls to third-party APIs etc.) and put the results back into CoValues, which subscribed clients automatically get notified about.

What's less clear is how you can trigger this work to happen.

- One option is to define traditional HTTP API handlers that use the Jazz Worker internally. This is helpful if you need to mutate Jazz state in response to HTTP requests such as for webhooks or non-Jazz API clients
- The other option is to have the Jazz Worker subscribe to CoValues which they will then collaborate on with clients.
    - A common pattern is to implement a state machine represented by a CoValue, where the client will do some state transitions (such as `draft -> ready`), which the worker will notice and then do some work in response, feeding the result back in a further state transition (such as `ready -> success & data`, or `ready -> failure & error details`).
    - This way, client and worker don't have to explicitly know about each other or communicate directly, but can rely on Jazz as a communication mechanism - with computation progressing in a distributed manner wherever and whenever possible.

---

### svelte Implementation

# Svelte Installation

Jazz can be used with Svelte or in a SvelteKit app.

To add some Jazz to your Svelte app, you can use the following steps:

1. Install Jazz dependencies

<CodeGroup>
```sh
pnpm install jazz-tools jazz-svelte
```
</CodeGroup>

2. Write your schema

See the [schema docs](/docs/schemas/covalues) for more information.

<CodeGroup>
```ts
// src/lib/schema.ts

export class MyProfile extends Profile {
  name = coField.string;
  counter = coField.number; // This will be publically visible
}

export class MyAccount extends Account {
  profile = coField.ref(MyProfile);

  // ...
}
```
</CodeGroup>

3. Set up the Provider in your root layout

<CodeGroup>
```svelte
<!-- src/routes/+layout.svelte -->
 <script lang="ts" module>
  // Register the Account schema so `useAccount` returns our custom `MyAppAccount`
  declare module 'jazz-svelte' {
    interface Register {
      Account: MyAccount;
    }
  }
</script>

<script lang="ts">
  import { JazzProvider } from 'jazz-svelte';

  // Example configuration for authentication and peer connection
  let sync = { peer: "wss://cloud.jazz.tools/?key=you@example.com" };
  let AccountSchema = MyAccount;
</script>

<JazzProvider {sync} {AccountSchema}>
  <App />
</JazzProvider>
```
</CodeGroup>

4. Use Jazz hooks in your components

<CodeGroup>
```svelte
<!-- src/routes/+page.svelte -->
<script lang="ts">
  import { useCoState, useAccount } from 'jazz-svelte';
  import { MyProfile } from './schema';

  const { me } = useAccount();

  const profile = $derived(useCoState(MyProfile, me._refs.profile.id));

  function increment() {
    if (!profile.current) return;
    profile.current.counter = profile.current.counter + 1;
  }
</script>

<button on:click={increment}>
  Count: {profile.current?.counter}
</button>
```
</CodeGroup>

For a complete example of Jazz with Svelte, check out our [file sharing example](https://github.com/gardencmp/jazz/tree/main/examples/file-share-svelte) which demonstrates, Passkey authentication, file uploads and access control.

---

### vue Implementation

# Sync and storage

## Using Jazz Cloud

Simply use `wss://cloud.jazz.tools/?key=...` as the sync server URL.

Jazz Cloud will
- sync CoValues in real-time between users and devices
- safely persist CoValues on redundant storage nodes with additional backups
- make use of geographically distributed cache nodes for low latency

### Free public alpha

- Jazz Cloud is free during the public alpha, with no strict usage limits
- We plan to keep a free tier, so you'll always be able to get started with zero setup
- See [Jazz Cloud pricing](/cloud#pricing) for more details
- ⚠️ Please use a valid email address as your API key.

    Your full sync server URL should look something like

    ```wss://cloud.jazz.tools/?key=you@example.com```

    Once we support per-app API keys, we'll email you an API key you can use instead.


## Running your own sync server

You can run your own sync server using:

<CodeGroup>
```sh
npx jazz-run sync
```
</CodeGroup>

And then use `ws://localhost:4200` as the sync server URL.

You can also run this simple sync server behind a proxy that supports WebSockets, for example to provide TLS.
In this case, provide the WebSocket endpoint your proxy exposes as the sync server URL.

### Command line options:

- `--port` / `-p` - the port to run the sync server on. Defaults to 4200.
- `--in-memory` - keep CoValues in-memory only and do sync only, no persistence. Persistence is enabled by default.
- `--db` - the path to the file where to store the data (SQLite). Defaults to `sync-db/storage.db`.

### Source code

The implementation of this simple sync server is available open-source [on GitHub](https://github.com/garden-co/jazz/blob/main/packages/jazz-run/src/startSyncServer.ts).



#### Node.JS / server workers

# Node.JS / server workers

The main detail to understand when using Jazz server-side is that Server Workers have Jazz `Accounts`, just like normal users do.

This lets you share CoValues with Server Workers, having precise access control by adding the Worker to `Groups` with specific roles just like you would with other users.

[See the full example here.](https://github.com/garden-co/jazz/tree/main/examples/jazz-paper-scissors)

## Generating credentials

Server Workers typically have static credentials, consisting of a public Account ID and a private Account Secret.

To generate new credentials for a Server Worker, you can run:

<CodeGroup>
```sh
npx jazz-run account create --name "My Server Worker"
```
</CodeGroup>

The name will be put in the public profile of the Server Worker's `Account`, which can be helpful when inspecting metadata of CoValue edits that the Server Worker has done.

## Storing & providing credentials

Server Worker credentials are typically stored and provided as environmental variables.

**Take extra care with the Account Secret &mdash; handle it like any other secret environment variable such as a DB password.**

## Starting a server worker

You can use `startWorker` from `jazz-nodejs` to start a Server Worker. Similarly to setting up a client-side Jazz context, it:

- takes a custom `AccountSchema` if you have one (for example, because the worker needs to store information in it's private account root)
- takes a URL for a sync & storage server

`startWorker` expects credentials in the `JAZZ_WORKER_ACCOUNT` and `JAZZ_WORKER_SECRET` environment variables by default (as printed by `npx account create ...`), but you can also pass them manually as `accountID` and `accountSecret` parameters if you get them from elsewhere.

<CodeGroup>
```ts twoslash
class MyWorkerAccount extends Account {}
// ---cut---

const { worker } = await startWorker({
  AccountSchema: MyWorkerAccount,
  syncServer: 'wss://cloud.jazz.tools/?key=you@example.com',
});
```
</CodeGroup>

`worker` acts like `me` (as returned by `useAccount` on the client) - you can use it to:

- load/subscribe to CoValues: `MyCoValue.subscribe(id, worker, {...})`
- create CoValues & Groups `const val = MyCoValue.create({...}, { owner: worker })`

## Using CoValues instead of requests

Just like traditional backend functions, you can use Server Workers to do useful stuff (computations, calls to third-party APIs etc.) and put the results back into CoValues, which subscribed clients automatically get notified about.

What's less clear is how you can trigger this work to happen.

- One option is to define traditional HTTP API handlers that use the Jazz Worker internally. This is helpful if you need to mutate Jazz state in response to HTTP requests such as for webhooks or non-Jazz API clients
- The other option is to have the Jazz Worker subscribe to CoValues which they will then collaborate on with clients.
    - A common pattern is to implement a state machine represented by a CoValue, where the client will do some state transitions (such as `draft -> ready`), which the worker will notice and then do some work in response, feeding the result back in a further state transition (such as `ready -> success & data`, or `ready -> failure & error details`).
    - This way, client and worker don't have to explicitly know about each other or communicate directly, but can rely on Jazz as a communication mechanism - with computation progressing in a distributed manner wherever and whenever possible.



#### Providers

### react-native-expo Implementation

# Providers

`<JazzProvider />` is the core component that connects your Expo application to Jazz. It handles:

- **Data Synchronization**: Manages connections to peers and the Jazz cloud
- **Local Storage**: Persists data locally between app sessions
- **Schema Types**: Provides APIs for the [AccountSchema](/docs/schemas/accounts-and-migrations)
- **Authentication**: Connects your authentication system to Jazz

## Setting up the provider

Wrap your app components with the `<JazzProvider />` component:

<CodeGroup>
```tsx twoslash
// @noErrors: 2307 7031 2304 2686 2664
// App.tsx

export function MyJazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=you@example.com" }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzProvider>
  );
}

// Register the Account schema so `useAccount` returns our custom `MyAppAccount`
declare module "jazz-expo" {
  interface Register {
    Account: MyAppAccount;
  }
}
```
</CodeGroup>

## Provider Options

- `kvStore`
  - `ExpoSecureStoreAdapter` (default)
- `AccountSchema`
  - `Account` (default)
- `CryptoProvider`
  - `PureJSCrypto` (default) - Pure JavaScript crypto provider
  - `RNQuickCrypto` - C++ accelerated crypto provider

## Authentication in the Provider

`<JazzProvider />` works with various authentication methods, with PassphraseAuth being the easiest way to get started for development and testing. For authentication details, refer to our [Authentication Overview](/docs/authentication/overview) guide.

The authentication hooks must always be used inside the `<JazzProvider />` component.

Implementing PassphraseAuth is straightforward:

1. Import the [wordlist](https://github.com/bitcoinjs/bip39/tree/a7ecbfe2e60d0214ce17163d610cad9f7b23140c/src/wordlists) for generating recovery phrases
2. Use the `usePassphraseAuth` hook to handle authentication 
3. Create simple registration and sign-in screens

<CodeGroup>
```tsx twoslash
// @noErrors: 2307
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
// ---cut-before---
// Example with PassphraseAuth

function JazzAuthentication({ children }: { children: ReactNode }) {
  const auth = usePassphraseAuth({
    wordlist: englishWordlist,
  });

  // If the user is already signed in, render the App
  if (auth.state === "signedIn") {
    return children
  }
  
  // Otherwise, show a sign-in screen
  return <SignInScreen auth={auth} />;
}

function AuthenticatedProvider({ children }: { children: ReactNode }) {
  return (
    <JazzProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=your-api-key" }}
    >
      <JazzAuthentication>
        {children}
      </JazzAuthentication>
    </JazzProvider>
  );
}
```
</CodeGroup>

For a complete example, see the [Expo Chat Demo](https://github.com/garden-co/jazz/tree/main/examples/chat-rn-expo).

## Local Persistence

Jazz for Expo includes built-in local persistence using SQLite. Following Expo's best practices, the Expo implementation uses:

- **Database Storage**: `expo-sqlite` - Expo's official SQLite module
- **Key-Value Storage**: `expo-secure-store` - Expo's secure storage system

Local persistence is enabled by default with no additional configuration required. Your data will automatically persist across app restarts.

---

### react-native Implementation

# Providers

`<JazzProvider />` is the core component that connects your React Native application to Jazz. It handles:

- **Data Synchronization**: Manages connections to peers and the Jazz cloud
- **Local Storage**: Persists data locally between app sessions
- **Schema Types**: Provides APIs for the [AccountSchema](/docs/schemas/accounts-and-migrations)
- **Authentication**: Connects your authentication system to Jazz

## Setting up the provider

Wrap your app components with the `<JazzProvider />` component:

<CodeGroup>
```tsx twoslash
// @noErrors: 2307 2686 2664
// App.tsx

export function MyJazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=you@example.com" }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzProvider>
  );
}

// Register the Account schema so `useAccount` returns our custom `MyAppAccount`
declare module "jazz-react-native" {
  interface Register {
    Account: MyAppAccount;
  }
}
```
</CodeGroup>

## Provider Options

- `kvStore`
  - `MMKVStoreAdapter` (default)
- `AccountSchema`
  - `Account` (default)
- `CryptoProvider`
  - `PureJSCrypto` (default) - Pure JavaScript crypto provider
  - `RNQuickCrypto` - C++ accelerated crypto provider

## Authentication in the Provider

`<JazzProvider />` works with various authentication methods, with PassphraseAuth being the easiest way to get started for development and testing. For authentication details, refer to our [Authentication Overview](/docs/authentication/overview) guide.

The authentication hooks must always be used inside the `<JazzProvider />` component.

Implementing PassphraseAuth is straightforward:

1. Import the [wordlist](https://github.com/bitcoinjs/bip39/tree/a7ecbfe2e60d0214ce17163d610cad9f7b23140c/src/wordlists) for generating recovery phrases
2. Use the `usePassphraseAuth` hook to handle authentication 
3. Create simple registration and sign-in screens

<CodeGroup>
```tsx twoslash
// @noErrors: 2307
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
// ---cut-before---
// Example with PassphraseAuth

function JazzAuthentication({ children }: { children: ReactNode }) {
  const auth = usePassphraseAuth({
    wordlist: englishWordlist,
  });

  // If the user is already signed in, render the App
  if (auth.state === "signedIn") {
    return children
  }
  
  // Otherwise, show a sign-in screen
  return <SignInScreen auth={auth} />;
}

function AuthenticatedProvider({ children }: { children: ReactNode }) {
  return (
    <JazzProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=your-api-key" }}
    >
      <JazzAuthentication>
        {children}
      </JazzAuthentication>
    </JazzProvider>
  );
}
```
</CodeGroup>

## Local Persistence

Jazz for React Native includes built-in local persistence using SQLite. This implementation uses:

- **Database Storage**: `@op-engineering/op-sqlite` - A high-performance SQLite implementation
- **Key-Value Storage**: `react-native-mmkv` - A fast key-value storage system

Local persistence is enabled by default with no additional configuration required. Your data will automatically persist across app restarts.

---

### react Implementation

# Providers

`<JazzProvider />` is the core component that connects your React application to Jazz. It handles:

- **Data Synchronization**: Manages connections to peers and the Jazz cloud
- **Local Storage**: Persists data locally between app sessions
- **Schema Types**: Provides APIs for the [AccountSchema](/docs/schemas/accounts-and-migrations)
- **Authentication**: Connects your authentication system to Jazz

Our [Chat example app](https://jazz.tools/examples#chat) provides a complete implementation of JazzProvider with authentication and real-time data sync.

## Setting up the Provider

The `<JazzProvider />` accepts several configuration options:

<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
});

export const AccountRoot = co.map({
  todos: co.list(TodoItem),
});

export const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});
// @filename: app.tsx
// ---cut---
// App.tsx

export function MyApp({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=your-api-key",
        when: "always" // When to sync: "always", "never", or "signedUp"
      }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzProvider>
  );
}
```
</CodeGroup>

## Provider Options

### Sync Options

The `sync` property configures how your application connects to the Jazz network:

<CodeGroup>
```tsx twoslash

const syncConfig: SyncConfig = {
  // Connection to Jazz Cloud or your own sync server
  peer: "wss://cloud.jazz.tools/?key=your-api-key",

  // When to sync: "always" (default), "never", or "signedUp"
  when: "always",
}
```
</CodeGroup>

See [Authentication States](/docs/authentication/authentication-states#controlling-sync-for-different-authentication-states) for more details on how the `when` property affects synchronization based on authentication state.

### Account Schema

The `AccountSchema` property defines your application's account structure:

<CodeGroup>
```tsx twoslash

// @filename: schema.ts

export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
});

export const AccountRoot = co.map({
  todos: co.list(TodoItem),
});

export const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});

// @filename: app.tsx

const syncConfig: SyncConfig = {
  peer: "wss://cloud.jazz.tools/?key=your-api-key",
  when: "always",
}

// ---cut---
// app.tsx

export function MyApp ({ children }: { children: React.ReactNode }) {
  // Use in provider
  return (
    <JazzProvider
      sync={syncConfig}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzProvider>
  );
}
```
</CodeGroup>

### Additional Options

The provider accepts these additional options:

<CodeGroup>
```tsx twoslash

const syncConfig: SyncConfig = {
  peer: "wss://cloud.jazz.tools/?key=your-api-key",
  when: "always",
}

// ---cut---
// app.tsx
export function MyApp ({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={syncConfig}

      // Enable guest mode for account-less access
      guestMode={false}

      // Set default name for new user profiles
      defaultProfileName="New User"

      // Handle user logout
      onLogOut={() => {
        console.log("User logged out");
      }}

      // Handle anonymous account data when user logs in to existing account
      onAnonymousAccountDiscarded={(account) => {
        console.log("Anonymous account discarded", account.id);
        // Migrate data here
        return Promise.resolve();
      }}
    >
      {children}
    </JazzProvider>
  );
}
```
</CodeGroup>

See [Authentication States](/docs/authentication/authentication-states) for more information on authentication states, guest mode, and handling anonymous accounts.

## Authentication

`<JazzProvider />` works with various authentication methods to enable users to access their data across multiple devices. For a complete guide to authentication, see our [Authentication Overview](/docs/authentication/overview).

## Need Help?

If you have questions about configuring the Jazz Provider for your specific use case, [join our Discord community](https://discord.gg/utDMjHYg42) for help.

---

### svelte Implementation

# Providers

`<JazzProvider />` is the core component that connects your Svelte application to Jazz. It handles:

- **Data Synchronization**: Manages connections to peers and the Jazz cloud
- **Local Storage**: Persists data locally between app sessions
- **Schema Types**: Provides APIs for the [AccountSchema](/docs/schemas/accounts-and-migrations)
- **Authentication**: Connects your authentication system to Jazz

Our [File Share example app](https://github.com/garden-co/jazz/blob/main/examples/file-share-svelte/src/routes/%2Blayout.svelte) provides an implementation of JazzProvider with authentication and real-time data sync.

## Setting up the Provider

The `<JazzProvider />` accepts several configuration options:

<CodeGroup>
```svelte
<!-- src/routes/+layout.svelte -->
<script lang="ts" module>
  // Register the Account schema so `useAccount` returns our custom `MyAppAccount`
  declare module 'jazz-svelte' {
    interface Register {
      Account: MyAppAccount;
    }
  }
</script>

<script lang="ts">
  import { JazzProvider } from "jazz-svelte";
  import { MyAppAccount } from "$lib/schema";
  let { children } = $props();
</script>

<JazzProvider
  sync={{ 
    peer: "wss://cloud.jazz.tools/?key=your-api-key",
    when: "always" // When to sync: "always", "never", or "signedUp"
  }}
  AccountSchema={MyAppAccount}
>
  {@render children()}
</JazzProvider>
```
</CodeGroup>

## Provider Options

### Sync Options

The `sync` property configures how your application connects to the Jazz network:


<CodeGroup>
```ts twoslash
// @filename: src/routes/layout.svelte

// ---cut---

const syncConfig: SyncConfig = {
  // Connection to Jazz Cloud or your own sync server
  peer: "wss://cloud.jazz.tools/?key=your-api-key",
  
  // When to sync: "always" (default), "never", or "signedUp"
  when: "always",
}
```
</CodeGroup>


See [Authentication States](/docs/authentication/authentication-states#controlling-sync-for-different-authentication-states) for more details on how the `when` property affects synchronization based on authentication state.

### Account Schema

The `AccountSchema` property defines your application's account structure:

<CodeGroup>
```svelte
<!-- src/routes/+layout.svelte -->>
<script lang="ts" module>
  // Register the Account schema so `useAccount` returns our custom `MyAppAccount`
  declare module 'jazz-svelte' {
    interface Register {
      Account: MyAppAccount;
    }
  }
</script>

<script lang="ts">
  import { JazzProvider } from "jazz-svelte";
  import { MyAppAccount } from "$lib/schema";
  let { children } = $props();
</script>

<JazzProvider
  sync={syncConfig}
  AccountSchema={MyAppAccount}
>
  {@render children()}
</JazzProvider>
```
</CodeGroup>

### Additional Options

The provider accepts these additional options:

<CodeGroup>
```svelte
<!-- src/routes/+layout.svelte -->
<script lang="ts">
  import { JazzProvider } from "jazz-svelte";
  import { syncConfig } from "$lib/syncConfig";
  let { children } = $props();
  
  // Enable guest mode for account-less access
  const guestMode = false; 
  
  // Default name for new user profiles
  const defaultProfileName = "New User"; 
  
  // Handle user logout
  const onLogOut = () => {
    console.log("User logged out");
  };

  // Handle anonymous account data when user logs in to existing account
  const onAnonymousAccountDiscarded = (account) => {
    console.log("Anonymous account discarded", account.id);
    // Migrate data here
    return Promise.resolve();
  };
</script>

<JazzProvider
  sync={syncConfig}
  {guestMode}
  {defaultProfileName}
  {onLogOut}
  {onAnonymousAccountDiscarded}
>
  {@render children}
</JazzProvider>
```
</CodeGroup>

See [Authentication States](/docs/authentication/authentication-states) for more information on authentication states, guest mode, and handling anonymous accounts.

## Authentication

`<JazzProvider />` works with various authentication methods to enable users to access their data across multiple devices. For a complete guide to authentication, see our [Authentication Overview](/docs/authentication/overview).

## Need Help?

If you have questions about configuring the Jazz Provider for your specific use case, [join our Discord community](https://discord.gg/utDMjHYg42) for help.



### Tools

#### AI tools

# Using AI to build Jazz apps

AI tools, particularly large language models (LLMs), can accelerate your development with Jazz. Searching docs, responding to questions and even helping you write code are all things that LLMs are starting to get good at.

However, Jazz is a rapidly evolving framework, so sometimes AI might get things a little wrong.

To help the LLMs, we provide the Jazz documentation in a txt file that is optimized for use with AI tools, like Cursor.

<FileDownloadLink href="/llms-full.txt">llms-full.txt</FileDownloadLink>

## Setting up AI tools

Every tool is different, but generally, you'll need to either paste the contents of the [llms-full.txt](https://jazz.tools/llms-full.txt) file directly in your prompt, or attach the file to the tool.

### ChatGPT and v0

Upload the txt file in your prompt.

![ChatGPT prompt with llms-full.txt attached](/chatgpt-with-llms-full-txt.jpg)

### Cursor

1. Go to Settings > Cursor Settings > Features > Docs
2. Click "Add new doc"
3. Enter the following URL:

<CodeGroup>
```
https://jazz.tools/llms-full.txt
```
</CodeGroup>

## llms.txt convention

We follow the llms.txt [proposed standard](https://llmstxt.org/) for providing documentation to AI tools at inference time that helps them understand the context of the code you're writing.

## Limitations and considerations

AI is amazing, but it's not perfect. What works well this week could break next week (or be twice as good).

We're keen to keep up with changes in tooling to help support you building the best apps, but if you need help from humans (or you have issues getting set up), please let us know on [Discord](https://discord.gg/utDMjHYg42).



#### create-jazz-app

# create-jazz-app

Jazz comes with a CLI tool that helps you quickly scaffold new Jazz applications. There are two main ways to get started:

1. **Starter templates** - Pre-configured setups to start you off with your preferred framework
2. **Example apps** - Extend one of our [example applications](https://jazz.tools/examples) to build your project

## Quick Start with Starter Templates

Create a new Jazz app from a starter template in seconds:

<CodeGroup>
```bash
npx create-jazz-app@latest --api-key you@example.com
```
</CodeGroup>

This launches an interactive CLI that guides you through selecting:
- Pre-configured frameworks and authentication methods (See [Available Starters](#available-starters))
- Package manager
- Project name
- Jazz Cloud API key (optional) - Provides seamless sync and storage for your app

## Command Line Options

If you know what you want, you can specify options directly from the command line:

<CodeGroup>
```bash
# Basic usage with project name
npx create-jazz-app@latest my-app --framework react --api-key you@example.com

# Specify a starter template
npx create-jazz-app@latest my-app --starter react-passkey-auth --api-key you@example.com

# Specify example app
npx create-jazz-app@latest my-app --example chat --api-key you@example.com
```
</CodeGroup>

### Available Options

- `directory` - Directory to create the project in (defaults to project name)
- `-f, --framework` - Framework to use (React, React Native, Svelte, Vue)
- `-s, --starter` - Starter template to use
- `-e, --example` - Example project to use
- `-p, --package-manager` - Package manager to use (npm, yarn, pnpm, bun, deno)
- `-k, --api-key` - Jazz Cloud API key (during our [free public alpha](/docs/react/sync-and-storage#free-public-alpha), you can use your email as the API key)
- `-h, --help` - Display help information

## Start From an Example App

Want to start from one of [our example apps](https://jazz.tools/examples)? Our example apps include specific examples of features and use cases. They demonstrate real-world patterns for building with Jazz. Use one as your starting point:

<CodeGroup>
```bash
npx create-jazz-app@latest --example chat
```
</CodeGroup>

## Available Starters

Starter templates are minimal setups that include the basic configuration needed to get started with Jazz. They're perfect when you want a clean slate to build on.

Choose from these ready-to-use starter templates:

- `react-passkey-auth` - React with Passkey authentication (easiest to start with)
- `react-clerk-auth` - React with Clerk authentication
- `vue-demo-auth` - Vue with Demo authentication
- `svelte-passkey-auth` - Svelte with Passkey authentication
- `rn-clerk-auth` - React Native with Clerk authentication

Run `npx create-jazz-app --help` to see the latest list of available starters.

## What Happens Behind the Scenes

When you run `create-jazz-app`, we'll:

1. Ask for your preferences (or use your command line arguments)
2. Clone the appropriate starter template
3. Update dependencies to their latest versions
4. Install all required packages
5. Set up your project and show next steps

## Requirements

- Node.js 14.0.0 or later
- Your preferred package manager (npm, yarn, pnpm, bun, or deno)



#### Inspector

# Jazz Inspector

[Jazz Inspector](https://inspector.jazz.tools) is a tool to visually inspect a Jazz account or other CoValues.

For now, you can get your account credentials from the `jazz-logged-in-secret` local storage key from within your Jazz app.

[https://inspector.jazz.tools](https://inspector.jazz.tools)

<ContentByFramework framework={["react", "svelte", "vue", "vanilla"]}>
## Exporting current account to Inspector from your app [!framework=react,svelte,vue,vanilla]

In development mode, you can launch the Inspector from your Jazz app to inspect your account by pressing `Cmd+J`.

## Embedding the Inspector widget into your app [!framework=react,svelte,vue,vanilla]

Alternatively, you can embed the Inspector directly into your app, so you don't need to open a separate window.

Install the package.

<ContentByFramework framework="react">
<CodeGroup>
```sh
npm install jazz-inspector
```
</CodeGroup>

Render the component within your `JazzProvider`.

<CodeGroup>
```tsx

<JazzProvider>
 // [!code ++]
  <JazzInspector />
</JazzProvider>
```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework={["svelte", "vue", "vanilla"]}>
<CodeGroup>
```sh
npm install jazz-inspector-element
```
</CodeGroup>

Render the component.

<CodeGroup>
```ts

document.body.appendChild(document.createElement("jazz-inspector"))
```
</CodeGroup>

Or

<CodeGroup>
```tsx

<jazz-inspector />
```
</CodeGroup>

</ContentByFramework>

This will show the Inspector launch button on the right of your page.

</ContentByFramework>

<ContentByFramework framework="react">
### Positioning the Inspector button [!framework=react]

You can also customize the button position with the following options:

- right (default)
- left
- bottom right
- bottom left
- top right
- top left

For example:

<CodeGroup>
```tsx
<JazzInspector position="bottom left"/>
```
</CodeGroup>

<div className="bg-stone-200 flex items-center justify-center h-72 relative dark:bg-stone-900 mt-5">
  <span>Your app</span>

  <button className="size-10 p-1.5 border rounded-lg left-4 bottom-4 absolute bg-white shadow-sm" type="button">
    <JazzIcon className="w-full h-auto"/>
  </button>
</div>
</ContentByFramework>

<ContentByFramework framework="react">
Check out the [music player app](https://github.com/garden-co/jazz/blob/main/examples/music-player/src/2_main.tsx) for a full example.
</ContentByFramework>

<ContentByFramework framework="svelte">
Check out the [file share app](https://github.com/garden-co/jazz/blob/main/examples/file-share-svelte/src/src/routes/%2Blayout.svelte) for a full example.
</ContentByFramework>



### Upgrade guides

#### 0.14.0 - Zod-based schemas

# Jazz 0.14.0 Introducing Zod-based schemas

We're excited to move from our own schema syntax to using Zod v4.

This is the first step in a series of releases to make Jazz more familiar and to make CoValues look more like regular data structures.

**Note: This is a huge release that we're still cleaning up and documenting.**

<small className="leading-tight">
We're still in the process of:
- updating all our docs
- double-checking all our framework bindings
- completing all the details of this upgrade guide
</small>

If you see something broken, please let us know on [Discord](https://discord.gg/utDMjHYg42) and check back in a couple hours.

Thanks for your patience!

## Overview:

So far, Jazz has relied on our own idiosyncratic schema definition syntax where you had to extend classes and be careful to use `co.ref` for references.

<CodeGroup>
```ts
// BEFORE

export class Message extends CoMap {
  text = co.ref(CoPlainText);
  image = co.optional.ref(ImageDefinition);
  important = co.boolean;
}

export class Chat extends CoList.Of(co.ref(Message)) {}
```
</CodeGroup>

While this had certain ergonomic benefits it relied on unclean hacks to work.

In addition, many of our adopters expressed a preference for avoiding class syntax, and LLMs consistently expected to be able to use Zod.

For this reason, we completely overhauled how you define and use CoValue schemas:

<CodeGroup>
```ts twoslash
// AFTER

export const Message = co.map({
  text: co.plainText(),
  image: z.optional(co.image()),
  important: z.boolean(),
});

export const Chat = co.list(Message);
```
</CodeGroup>

## Major breaking changes

### Schema definitions

You now define CoValue schemas using two new exports from `jazz-tools`:

- a new `co` definer that mirrors Zod's object/record/array syntax to define CoValue types
  - `co.map()`, `co.record()`, `co.list()`, `co.feed()`
  - `co.account()`, `co.profile()`
  - `co.plainText()`, `co.richText()`,
  - `co.fileStream()`, `co.image()`
  - see the updated [Defining CoValue Schemas](/docs/schemas/covalues)
- `z` re-exported from Zod v4
  - primitives like `z.string()`, `z.number()`, `z.literal()`
    - **note**: additional constraints like `z.min()` and `z.max()` are not yet enforced, we'll add validation in future releases
  - complex types like `z.object()` and `z.array()` to define JSON-like fields without internal collaboration
  - combinators like `z.optional()` and `z.discriminatedUnion()`
    - these also work on CoValue types!
  - see the updated [Docs on Primitive Fields](/docs/schemas/covalues#primitive-fields),
    [Docs on Optional References](/docs/schemas/covalues#optional-references)
    and [Docs on Unions of CoMaps](/docs/schemas/covalues#unions-of-comaps-declaration)

Similar to Zod v4's new object syntax, recursive and mutually recursive types are now [much easier to express](/docs/react/schemas/covalues#recursive-references).

### How to pass loaded CoValues

<ContentByFramework framework={["react", "react-native", "vue", "vanilla", "react-native-expo"]}>
Calls to `useCoState()` work just the same, but they return a slightly different type than before.

And while you can still read from the type just as before...

<CodeGroup>
```tsx twoslash
// ---cut---

const Pet = co.map({
  name: z.string(),
  age: z.number(),
});
type Pet = co.loaded<typeof Pet>;

const Person = co.map({
  name: z.string(),
  age: z.number(),
  pets: co.list(Pet),
});
type Person = co.loaded<typeof Person>;

function MyComponent({ id }: { id: string }) {
  const person = useCoState(Person, id);

  return person && <PersonName person={person} />;
}

function PersonName({ person }: {
  person: Person
}) {
  return <div>{person.name}</div>;
}
```
</CodeGroup>

`co.loaded` can also take a second argument to specify the loading depth of the expected CoValue, mirroring the `resolve` options for `useCoState`, `load`, `subscribe`, etc.

<CodeGroup>
```tsx twoslash
// ---cut---

const Pet = co.map({
  name: z.string(),
  age: z.number(),
});
type Pet = co.loaded<typeof Pet>;

const Person = co.map({
  name: z.string(),
  age: z.number(),
  pets: co.list(Pet),
});
type Person = co.loaded<typeof Person>;

function MyComponent({ id }: { id: string }) {
  const personWithPets = useCoState(Person, id, {
    resolve: { pets: { $each: true } }  // [!code ++]
  });

  return personWithPets && <PersonAndFirstPetName person={personWithPets} />;
}

function PersonAndFirstPetName({ person }: {
  person: co.loaded<typeof Person, { pets: { $each: true } }> // [!code ++]
}) {
  return <div>{person.name} & {person.pets[0].name}</div>;
}
```
</CodeGroup>
</ContentByFramework>
<ContentByFramework framework="svelte">
We've removed the `useCoState`, `useAccount` and `useAccountOrGuest` hooks.

You should now use the `CoState` and `AccountCoState` reactive classes instead. These provide greater stability and are significantly easier to work with.

Calls to `new CoState()` work just the same, but they return a slightly different type than before.

And while you can still read from the type just as before...

<CodeGroup>
```ts twoslash filename="schema.ts"
// @filename: schema.ts

const Pet = co.map({
  name: z.string(),
  age: z.number(),
});
type Pet = co.loaded<typeof Pet>;

const Person = co.map({
  name: z.string(),
  age: z.number(),
  pets: co.list(Pet),
});
type Person = co.loaded<typeof Person>;
``` 
```svelte twoslash filename="app.svelte"
// @filename: app.svelte
<script lang="ts">

const person = new CoState(Person, id);
</script>

<div>
  {person.current?.name}
</div>
```
</CodeGroup>

`co.loaded` can also take a second argument to specify the loading depth of the expected CoValue, mirroring the `resolve` options for `CoState`, `load`, `subscribe`, etc.

<CodeGroup>
```svelte twoslash
<script lang="ts" module>
  export type Props = {
    person: co.loaded<typeof Person, { pets: { $each: true } }>;  // [!code ++]
  };
</script>

<script lang="ts">
  import { Person } from './schema';
  
  let props: Props = $props();
</script>

<div>
  {props.person.name}
</div>
<ul>
  {#each props.person.pets as pet}
    <li>{pet.name}</li>
  {/each}
</ul>
```
</CodeGroup>
</ContentByFramework>

### Removing AccountSchema registration

We have removed the Typescript AccountSchema registration.

It was causing some deal of confusion to new adopters so we have decided to replace the magic inference with a more explicit approach.

<ContentByFramework framework={["react", "react-native", "vue", "vanilla", "react-native-expo"]}>
When using `useAccount` you should now pass the `Account` schema directly:

<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export const MyAccount = co.account({
  profile: co.profile(),
  root: co.map({})
});

// @filename: app.tsx
// ---cut---

function MyComponent() {
  const { me } = useAccount(MyAccount, {
    resolve: {
      profile: true,
    },
  });

  return <div>{me?.profile.name}</div>;
}
```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework="svelte">
When using `AccountCoState` you should now pass the `Account` schema directly:

<CodeGroup>
```svelte twoslash filename="app.svelte"
<script lang="ts">

const account = new AccountCoState(MyAccount, {
  resolve: {
    profile: true,
  },
});
</script>

<div>
  {account.current?.profile.name}
</div>
```
</CodeGroup>
</ContentByFramework>

### Defining migrations

Now account schemas need to be defined with `co.account()` and migrations can be declared using `withMigration()`:

<CodeGroup>
```ts twoslash

const Pet = co.map({
  name: z.string(),
  age: z.number(),
});

const MyAppRoot = co.map({
  pets: co.list(Pet),
});

const MyAppProfile = co.profile({
  name: z.string(),
  age: z.number().optional(),
});

export const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: MyAppProfile,
}).withMigration((account, creationProps?: { name: string }) => {
  if (account.root === undefined) {
    account.root = MyAppRoot.create({
      pets: co.list(Pet).create([]),
    });
  }

  if (account.profile === undefined) {
    const profileGroup = Group.create();
    profileGroup.addMember("everyone", "reader");

    account.profile = MyAppProfile.create({
      name: creationProps?.name ?? "New user",
    }, profileGroup);
  }
});
```
</CodeGroup>

### Defining Schema helper methods

TODO

## Minor breaking changes

### `_refs` and `_edits` are now potentially null

The type of `_refs` and `_edits` is now nullable.

<CodeGroup>
```ts twoslash 
// ---cut---
const Person = co.map({
  name: z.string(),
  age: z.number(),
});

const person = Person.create({ name: "John", age: 30 });

person._refs; // now nullable
person._edits; // now nullable
``` 
</CodeGroup>

### `members` and `by` now return basic `Account`

We have removed the Account schema registration, so now `members` and `by` methods now always return basic `Account`.

This means that you now need to rely on `useCoState` on them to load their using your account schema.

<CodeGroup>
```tsx twoslash

const Pet = co.map({
  name: z.string(),
  age: z.number(),
});

const MyAppRoot = co.map({
  pets: co.list(Pet),
});

const MyAppProfile = co.profile({
  name: z.string(),
  age: z.number().optional(),
});

export const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: MyAppProfile,
});

// ---cut---
function GroupMembers({ group }: { group: Group }) {
  const members = group.members;

  return (
    <div>
      {members.map((member) => (
        <GroupMemberDetails
          accountId={member.account.id}
          key={member.account.id}
        />
      ))}
    </div>
  );
}

function GroupMemberDetails({ accountId }: { accountId: string }) {
  const account = useCoState(MyAppAccount, accountId, {
    resolve: {
      profile: true,
      root: {
        pets: { $each: true },
      },
    },
  });

  return (
    <div>
      <div>{account?.profile.name}</div>
      <ul>{account?.root.pets.map((pet) => <li>{pet.name}</li>)}</ul>
    </div>
  );
}
```
</CodeGroup>



### Defining schemas

#### CoValues

# Defining schemas: CoValues

**CoValues ("Collaborative Values") are the core abstraction of Jazz.** They're your bread-and-butter datastructures that you use to represent everything in your app.

As their name suggests, CoValues are inherently collaborative, meaning **multiple users and devices can edit them at the same time.**

**Think of CoValues as "super-fast Git for lots of tiny data."**

- CoValues keep their full edit histories, from which they derive their "current state".
- The fact that this happens in an eventually-consistent way makes them [CRDTs](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).
- Having the full history also means that you often don't need explicit timestamps and author info - you get this for free as part of a CoValue's [edit metadata](/docs/using-covalues/history).

CoValues model JSON with CoMaps and CoLists, but also offer CoFeeds for simple per-user value feeds, and let you represent binary data with FileStreams.

## Start your app with a schema

Fundamentally, CoValues are as dynamic and flexible as JSON, but in Jazz you use them by defining fixed schemas to describe the shape of data in your app.

This helps correctness and development speed, but is particularly important...
- when you evolve your app and need migrations
- when different clients and server workers collaborate on CoValues and need to make compatible changes

Thinking about the shape of your data is also a great first step to model your app.

Even before you know the details of how your app will work, you'll probably know which kinds of objects it will deal with, and how they relate to each other.

In Jazz, you define schemas using `co` for CoValues and `z` (from [Zod](https://zod.dev/)) for their primitive fields.

<CodeGroup>
```ts twoslash
// schema.ts

const ListOfTasks = co.list(z.string());

export const TodoProject = co.map({
  title: z.string(),
  tasks: ListOfTasks,
});
```
</CodeGroup>

This gives us schema info that is available for type inference *and* at runtime.

Check out the inferred type of `project` in the example below, as well as the input `.create()` expects.

<CodeGroup>
```ts twoslash
// @filename: schema.ts

export const ListOfTasks = co.list(z.string());

export const TodoProject = co.map({
  title: z.string(),
  tasks: ListOfTasks,
});

// @filename: app.ts
// ---cut---
// app.ts

const project = TodoProject.create(
  {
    title: "New Project",
    tasks: ListOfTasks.create([], Group.create()),
  },
  Group.create()
);
```
</CodeGroup>

## Types of CoValues

### `CoMap` (declaration)

CoMaps are the most commonly used type of CoValue. They are the equivalent of JSON objects (Collaborative editing follows a last-write-wins strategy per-key).

You can either declare struct-like CoMaps:

<CodeGroup>
```ts twoslash
// schema.ts
// ---cut---
const Task = co.map({
  title: z.string(),
  completed: z.boolean(),
});
```
</CodeGroup>

Or record-like CoMaps (key-value pairs, where keys are always `string`):

<CodeGroup>
```ts twoslash
const Fruit = co.map({
  name: z.string(),
  color: z.string(),
});
// ---cut---
const ColorToHex = co.record(z.string(), z.string());

const ColorToFruit = co.record(z.string(), Fruit);
```
</CodeGroup>


See the corresponding sections for [creating](/docs/using-covalues/comaps#creating-comaps),
[subscribing/loading](/docs/using-covalues/subscription-and-loading),
[reading from](/docs/using-covalues/comaps#reading-from-comaps) and
[updating](/docs/using-covalues/comaps#updating-comaps) CoMaps.

### `CoList` (declaration)

CoLists are ordered lists and are the equivalent of JSON arrays. (They support concurrent insertions and deletions, maintaining a consistent order.)

You define them by specifying the type of the items they contain:

<CodeGroup>
```ts twoslash
const Task = co.map({
  title: z.string(),
  completed: z.boolean(),
});
// ---cut---
const ListOfColors = co.list(z.string());
const ListOfTasks = co.list(Task);
```
</CodeGroup>

See the corresponding sections for [creating](/docs/using-covalues/colists#creating-colists),
[subscribing/loading](/docs/using-covalues/subscription-and-loading),
[reading from](/docs/using-covalues/colists#reading-from-colists) and
[updating](/docs/using-covalues/colists#updating-colists) CoLists.

### `CoFeed` (declaration)

CoFeeds are a special CoValue type that represent a feed of values for a set of users/sessions (Each session of a user gets its own append-only feed).

They allow easy access of the latest or all items belonging to a user or their sessions. This makes them particularly useful for user presence, reactions, notifications, etc.

You define them by specifying the type of feed item:

<CodeGroup>
```ts twoslash
const Task = co.map({
  title: z.string(),
  completed: z.boolean(),
});
// ---cut---
const FeedOfTasks = co.feed(Task);
```
</CodeGroup>

See the corresponding sections for [creating](/docs/using-covalues/cofeeds#creating-cofeeds),
[subscribing/loading](/docs/using-covalues/subscription-and-loading),
[reading from](/docs/using-covalues/cofeeds#reading-from-cofeeds) and
[writing to](/docs/using-covalues/cofeeds#writing-to-cofeeds) CoFeeds.

### `FileStream` (declaration)

FileStreams are a special type of CoValue that represent binary data. (They are created by a single user and offer no internal collaboration.)

They allow you to upload and reference files.

You typically don't need to declare or extend them yourself, you simply refer to the built-in `co.fileStream()` from another CoValue:

<CodeGroup>
```ts twoslash
// ---cut---
const Document = co.map({
  title: z.string(),
  file: co.fileStream(),
});
```
</CodeGroup>

See the corresponding sections for [creating](/docs/using-covalues/filestreams#creating-filestreams),
[subscribing/loading](/docs/using-covalues/subscription-and-loading),
[reading from](/docs/using-covalues/filestreams#reading-from-filestreams) and
[writing to](/docs/using-covalues/filestreams#writing-to-filestreams) FileStreams.

**Note: For images, we have a special, higher-level `co.image()` helper, see [ImageDefinition](/docs/using-covalues/imagedef).**

### Unions of CoMaps (declaration)

You can declare unions of CoMaps that have discriminating fields, using `co.discriminatedUnion()`.

<CodeGroup>
```ts twoslash
// ---cut---

const ButtonWidget = co.map({
  type: z.literal("button"),
  label: z.string(),
});

const SliderWidget = co.map({
  type: z.literal("slider"),
  min: z.number(),
  max: z.number(),
});

const WidgetUnion = co.discriminatedUnion([ButtonWidget, SliderWidget]);
```
</CodeGroup>

See the corresponding sections for [creating](/docs/using-covalues/schemaunions#creating-schemaunions),
[subscribing/loading](/docs/using-covalues/subscription-and-loading) and
[narrowing](/docs/using-covalues/schemaunions#narrowing) SchemaUnions.

## CoValue field/item types

Now that we've seen the different types of CoValues, let's see more precisely how we declare the fields or items they contain.

### Primitive fields

You can declare primitive field types using `z` (re-exported in `jazz-tools` from [Zod](https://zod.dev/)):

<CodeGroup>
```ts twoslash

const Person = co.map({
  title: z.string(),
})

export const ListOfColors = co.list(z.string());
```
</CodeGroup>

Here's a quick overview of the primitive types you can use:

<CodeGroup>
```ts twoslash
// ---cut---
z.string();  // For simple strings
z.number();  // For numbers
z.boolean(); // For booleans
z.null();    // For null
z.date();    // For dates
z.literal(["waiting", "ready"]); // For enums
```
</CodeGroup>

Finally, for more complex JSON data, that you *don't want to be collaborative internally* (but only ever update as a whole), you can use more complex Zod types.

For example, you can use `z.object()` to represent an internally immutable position:

<CodeGroup>
```ts twoslash
// ---cut---
const Sprite = co.map({
  // assigned as a whole
  position: z.object({ x: z.number(), y: z.number() }),
});
```
</CodeGroup>

Or you could use a `z.tuple()`:

<CodeGroup>
```ts twoslash
// ---cut---
const Sprite = co.map({
  // assigned as a whole
  position: z.tuple([z.number(), z.number()]),
});
```
</CodeGroup>

### References to other CoValues

To represent complex structured data with Jazz, you form trees or graphs of CoValues that reference each other.

Internally, this is represented by storing the IDs of the referenced CoValues in the corresponding fields, but Jazz abstracts this away, making it look like nested CoValues you can get or assign/insert.

The important caveat here is that **a referenced CoValue might or might not be loaded yet,** but we'll see what exactly that means in [Subscribing and Deep Loading](/docs/using-covalues/subscription-and-loading).

In Schemas, you declare references by just using the schema of the referenced CoValue:

<CodeGroup>
```ts twoslash
// ---cut---
// schema.ts
const Person = co.map({
  name: z.string(),
});

const ListOfPeople = co.list(Person);

const Company = co.map({
  members: ListOfPeople,
});
```
</CodeGroup>

#### Optional References

You can make references optional with `z.optional()`:

<CodeGroup>
```ts twoslash
const Pet = co.map({
  name: z.string(),
});
// ---cut---
const Person = co.map({
  pet: z.optional(Pet),
});
```
</CodeGroup>

#### Recursive References

You can refer to the same schema from within itself using getters:

<CodeGroup>
```ts twoslash
// ---cut---
const Person = co.map({
  name: z.string(),
  get bestFriend() {
    return Person;
  }
});
```
</CodeGroup>

You can use the same technique for mutually recursive references, but you'll need to help TypeScript along:

<CodeGroup>
```ts twoslash
// ---cut---

const Person = co.map({
  name: z.string(),
  get friends(): CoListSchema<typeof Person> {
    return ListOfPeople;
  }
});

const ListOfPeople = co.list(Person);
```
</CodeGroup>

Note: similarly, if you use modifiers like `z.optional()` you'll need to help TypeScript along:

<CodeGroup>
```ts twoslash
// ---cut---
const Person = co.map({
  name: z.string(),
  get bestFriend(): z.ZodOptional<typeof Person> {
    return z.optional(Person);
  }
});
```
</CodeGroup>

### Helper methods

You should define helper methods of CoValue schemas separately, in standalone functions.

<CodeGroup>
```ts twoslash
function differenceInYears(date1: Date, date2: Date) {
  const diffTime = Math.abs(date1.getTime() - date2.getTime());
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24 * 365.25));
}
// ---cut---
const Person = co.map({
  firstName: z.string(),
  lastName: z.string(),
  dateOfBirth: z.date(),
})
type Person = co.loaded<typeof Person>;

export function getPersonFullName(person: Person) {
  return `${person.firstName} ${person.lastName}`;
}

export function getPersonAgeAsOf(person: Person, date: Date) {
  return differenceInYears(date, person.dateOfBirth);
}

const person = Person.create({
  firstName: "John",
  lastName: "Doe",
  dateOfBirth: new Date("1990-01-01"),
});

const fullName = getPersonFullName(person);
const age = getPersonAgeAsOf(person, new Date());
```
</CodeGroup>



#### Accounts & migrations

# Accounts & Migrations

## CoValues as a graph of data rooted in accounts

Compared to traditional relational databases with tables and foreign keys,
Jazz is more like a graph database, or GraphQL APIs &mdash;
where CoValues can arbitrarily refer to each other and you can resolve references without having to do a join.
(See [Subscribing & deep loading](/docs/using-covalues/subscription-and-loading)).

To find all data related to a user, the account acts as a root node from where you can resolve all the data they have access to.
These root references are modeled explicitly in your schema, distinguishing between data that is typically public
(like a user's profile) and data that is private (like their messages).

### `Account.root` - private data a user cares about

Every Jazz app that wants to refer to per-user data needs to define a custom root `CoMap` schema and declare it in a custom `Account` schema as the `root` field:

<CodeGroup>
```ts twoslash
const Chat = co.map({});
// ---cut---

const MyAppRoot = co.map({
  myChats: co.list(Chat),
});

export const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: co.profile(),
});
```
</CodeGroup>

### `Account.profile` - public data associated with a user

The built-in `Account` schema class comes with a default `profile` field, which is a CoMap (in a Group with `"everyone": "reader"` - so publicly readable permissions)
that is set up for you based on the username the `AuthMethod` provides on account creation.

Their pre-defined schemas roughly look like this:

<CodeGroup>
```ts twoslash
// @noErrors: 2416
// ---cut---
// ...somewhere in jazz-tools itself...
const Account = co.account({
  root: co.map({}),
  profile: co.profile({
    name: z.string(),
  }),
});
```
</CodeGroup>

If you want to keep the default `co.profile()` schema, but customise your account's private `root`, all you have to do is define a new `root` field in your account schema and use `co.profile()` without options:

<CodeGroup>
```ts twoslash
const Chat = co.map({});
// ---cut---
const MyAppRoot = co.map({ // [!code ++:3]
  myChats: co.list(Chat),
});

export const MyAppAccount = co.account({
  root: MyAppRoot, // [!code ++]
  profile: co.profile(),
});
```
</CodeGroup>

If you want to extend the `profile` to contain additional fields (such as an avatar `co.image()`), you can declare your own profile schema class using `co.profile({...})`:

<CodeGroup>
```ts twoslash
const Chat = co.map({});
// ---cut---
export const MyAppRoot = co.map({
  myChats: co.list(Chat),
});

export const MyAppProfile = co.profile({ // [!code ++:4]
  name: z.string(), // compatible with default Profile schema
  avatar: z.optional(co.image()),
});

export const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: MyAppProfile, // [!code ++]
});
```
</CodeGroup>

## Resolving CoValues starting at `profile` or `root`

<ContentByFramework framework="react">
To use per-user data in your app, you typically use `useAccount` somewhere in a high-level component, pass it your custom Account schema and specify which references to resolve using a resolve query (see [Subscribing & deep loading](/docs/using-covalues/subscription-and-loading)).

<CodeGroup>
```tsx twoslash

const Chat = co.map({});

const MyAppRoot = co.map({
  myChats: co.list(Chat),
});

const MyAppProfile = co.profile();

const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: MyAppProfile,
});

class ChatPreview extends React.Component<{ chat: Loaded<typeof Chat> }> {};
class ContactPreview extends React.Component<{ contact: Loaded<typeof MyAppAccount> }> {};
// ---cut---

function DashboardPageComponent() {
  const { me } = useAccount(MyAppAccount, { resolve: {
    profile: true,
    root: {
      myChats: { $each: true },
    }
  }});

  return (
    <div>
      <h1>Dashboard</h1>
      {me ? (
        <div>
          <p>Logged in as {me.profile.name}</p>
          <h2>My chats</h2>
          {me.root.myChats.map((chat) => (
            <ChatPreview key={chat.id} chat={chat} />
          ))}
        </div>
      ) : (
        "Loading..."
      )}
    </div>
  );
}

```
</CodeGroup>
</ContentByFramework>


## Populating and evolving `root` and `profile` schemas with migrations

As you develop your app, you'll likely want to

- initialise data in a user's `root` and `profile`
- add more data to your `root` and `profile` schemas

You can achieve both by overriding the `migrate()` method on your `Account` schema class.

### When migrations run

Migrations are run after account creation and every time a user logs in.
Jazz waits for the migration to finish before passing the account to your app's context.

### Initialising user data after account creation

<CodeGroup>
```ts twoslash
const Chat = co.map({});
const Bookmark = co.map({});

const MyAppRoot = co.map({
  myChats: co.list(Chat),
});

const MyAppProfile = co.profile({
  name: z.string(),
  bookmarks: co.list(Bookmark),
});
// ---cut---
export const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: MyAppProfile,
}).withMigration((account, creationProps?: { name: string }) => {
  // we specifically need to check for undefined,
  // because the root might simply be not loaded (`null`) yet
  if (account.root === undefined) {
    account.root = MyAppRoot.create({
      // Using a group to set the owner is always a good idea.
      // This way if in the future we want to share
      // this coValue we can do so easily.
      myChats: co.list(Chat).create([], Group.create()),
    });
  }

  if (account.profile === undefined) {
    const profileGroup = Group.create();
    // Unlike the root, we want the profile to be publicly readable.
    profileGroup.addMember("everyone", "reader");

    account.profile = MyAppProfile.create({
      name: creationProps?.name ?? "New user",
      bookmarks: co.list(Bookmark).create([], profileGroup),
    }, profileGroup);
  }
});
```
</CodeGroup>

### Adding/changing fields to `root` and `profile`

To add new fields to your `root` or `profile` schemas, amend their corresponding schema classes with new fields,
and then implement a migration that will populate the new fields for existing users (by using initial data, or by using existing data from old fields).

To do deeply nested migrations, you might need to use the asynchronous `ensureLoaded()` method before determining whether the field already exists, or is simply not loaded yet.

Now let's say we want to add a `myBookmarks` field to the `root` schema:

<CodeGroup>
```ts twoslash
const Chat = co.map({});
const Bookmark = co.map({});

const MyAppProfile = co.profile({
  name: z.string(),
  bookmarks: co.list(Bookmark),
});

// ---cut---
const MyAppRoot = co.map({
  myChats: co.list(Chat),
  myBookmarks: z.optional(co.list(Bookmark)), // [!code ++:1]
});


export const MyAppAccount = co.account({
  root: MyAppRoot,
  profile: MyAppProfile,
}).withMigration(async (account) => {
  if (account.root === undefined) {
    account.root = MyAppRoot.create({
      myChats: co.list(Chat).create([], Group.create()),
    });
  }

  // We need to load the root field to check for the myContacts field
  const { root } = await account.ensureLoaded({
    resolve: { root: true }
  });

  // we specifically need to check for undefined,
  // because myBookmarks might simply be not loaded (`null`) yet
  if (root.myBookmarks === undefined) { // [!code ++:3]
    root.myBookmarks = co.list(Bookmark).create([], Group.create());
  }
});
```
</CodeGroup>

{/*
 TODO: Add best practice: only ever add fields

 Note: explain and reassure that there will be more guardrails in the future
 https://github.com/garden-co/jazz/issues/1160
*/}



### Using CoValues

#### CoMaps

# CoMaps

CoMaps are key-value objects that work like JavaScript objects. You can access properties with dot notation and define typed fields that provide TypeScript safety. They're ideal for structured data that needs type validation.

## Creating CoMaps

CoMaps are typically defined with `co.map()` and specifying primitive fields using `z` (see [Defining schemas: CoValues](/docs/schemas/covalues) for more details on primitive fields):

<CodeGroup>
```ts twoslash
const Member = co.map({
  name: z.string(),
});
// ---cut---

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});
```
</CodeGroup>

You can create either struct-like CoMaps with fixed fields (as above) or record-like CoMaps for key-value pairs:

<CodeGroup>
```ts twoslash
// ---cut---
const Inventory = co.record(z.string(), z.number());
```
</CodeGroup>

To instantiate a CoMap:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const Member = co.map({
  name: z.string(),
});
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});
const Inventory = co.record(z.string(), z.number());
// ---cut---
const project = Project.create({
  name: "Spring Planting",
  startDate: new Date("2025-03-15"),
  status: "planning",
});

const inventory = Inventory.create({
  tomatoes: 48,
  basil: 12,
});
```
</CodeGroup>

### Ownership

When creating CoMaps, you can specify ownership to control access:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const memberAccount = await createJazzTestAccount();

const Member = co.map({
  name: z.string(),
});

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});

// ---cut---
// Create with default owner (current user)
const privateProject = Project.create({
  name: "My Herb Garden",
  startDate: new Date("2025-04-01"),
  status: "planning",
});

// Create with shared ownership
const gardenGroup = Group.create();
gardenGroup.addMember(memberAccount, "writer");

const communityProject = Project.create(
  {
    name: "Community Vegetable Plot",
    startDate: new Date("2025-03-20"),
    status: "planning",
  },
  { owner: gardenGroup },
);
```
</CodeGroup>

See [Groups as permission scopes](/docs/groups/intro) for more information on how to use groups to control access to CoMaps.

## Reading from CoMaps

CoMaps can be accessed using familiar JavaScript object notation:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const Member = co.map({
  name: z.string(),
});
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});
const project = Project.create(
  {
    name: "Spring Planting",
    startDate: new Date("2025-03-20"),
    status: "planning",
  },
);
// ---cut---
console.log(project.name);      // "Spring Planting"
console.log(project.status);    // "planning"
```
</CodeGroup>

### Handling Optional Fields

Optional fields require checks before access:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const Member = co.map({
  name: z.string(),
});
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});
const project = Project.create(
  {
    name: "Spring Planting",
    startDate: new Date("2025-03-20"),
    status: "planning"
  },
);
// ---cut---
if (project.coordinator) {
  console.log(project.coordinator.name);  // Safe access
}
```
</CodeGroup>

### Working with Record CoMaps

For record-type CoMaps, you can access values using bracket notation:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const Inventory = co.record(z.string(), z.number());
// ---cut---
const inventory = Inventory.create({
  tomatoes: 48,
  peppers: 24,
  basil: 12
});

console.log(inventory["tomatoes"]);  // 48
```
</CodeGroup>

## Updating CoMaps

Updating CoMap properties uses standard JavaScript assignment:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const Member = co.map({
  name: z.string(),
});
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});
const Inventory = co.record(z.string(), z.number());
const project = Project.create(
  {
    name: "Spring Planting",
    startDate: new Date("2025-03-20"),
    status: "planning"
  },
);
// ---cut---
project.name = "Spring Vegetable Garden";    // Update name
project.startDate = new Date("2025-03-20");  // Update date
```
</CodeGroup>

### Type Safety

CoMaps are fully typed in TypeScript, giving you autocomplete and error checking:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const Member = co.map({
  name: z.string(),
});
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});
const Inventory = co.record(z.string(), z.number());
const project = Project.create(
  {
    name: "Spring Planting",
    startDate: new Date("2025-03-20"),
    status: "planning"
  },
);
// ---cut---
project.name = "Spring Vegetable Planting";  // ✓ Valid string
// @errors: 2322
project.startDate = "2025-03-15"; // ✗ Type error: expected Date
```
</CodeGroup>


### Deleting Properties

You can delete properties from CoMaps:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const Member = co.map({
  name: z.string(),
});
const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: z.optional(Member),
});
const Inventory = co.record(z.string(), z.number());
const project = Project.create(
  {
    name: "Spring Planting",
    startDate: new Date("2025-03-20"),
    status: "planning"
  },
);
const inventory = Inventory.create({
  tomatoes: 48,
  peppers: 24,
  basil: 12
});
// ---cut---
delete inventory["basil"];  // Remove a key-value pair

// For optional fields in struct-like CoMaps
project.coordinator = undefined;  // Remove the reference
```
</CodeGroup>

## Best Practices

### Structuring Data

- Use struct-like CoMaps for entities with fixed, known properties
- Use record-like CoMaps for dynamic key-value collections
- Group related properties into nested CoMaps for better organization

### Common Patterns

#### Helper methods

You should define helper methods of CoValue schemas separately, in standalone functions:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
// ---cut---

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  endDate: z.optional(z.date()),
});
type Project = co.loaded<typeof Project>;

export function isProjectActive(project: Project) {
  const now = new Date();
  return now >= project.startDate && (!project.endDate || now <= project.endDate);
}

export function formatProjectDuration(project: Project, format: "short" | "full") {
  const start = project.startDate.toLocaleDateString();
  if (!project.endDate) {
    return format === "full"
        ? `Started on ${start}, ongoing`
        : `From ${start}`;
  }

  const end = project.endDate.toLocaleDateString();
  return format === "full"
    ? `From ${start} to ${end}`
    : `${(project.endDate.getTime() - project.startDate.getTime()) / 86400000} days`;
}

const project = Project.create({
  name: "My project",
  startDate: new Date("2025-04-01"),
  endDate: new Date("2025-04-04"),
});

console.log(isProjectActive(project)); // false
console.log(formatProjectDuration(project, "short")); // "3 days"
```
</CodeGroup>



#### CoLists

# CoLists

CoLists are ordered collections that work like JavaScript arrays. They provide indexed access, iteration methods, and length properties, making them perfect for managing sequences of items.

## Creating CoLists

CoLists are defined by specifying the type of items they contain:

<CodeGroup>
```ts twoslash
const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});

// ---cut---

const ListOfResources = co.list(z.string());

const ListOfTasks = co.list(Task);
```
</CodeGroup>

To create a `CoList`:

<CodeGroup>
```ts twoslash

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});
// ---cut---
// Create an empty list
const resources = co.list(z.string()).create([]);

// Create a list with initial items
const tasks = co.list(Task).create([
  Task.create({ title: "Prepare soil beds", status: "in-progress" }),
  Task.create({ title: "Order compost", status: "todo" })
]);
```
</CodeGroup>

### Ownership

Like other CoValues, you can specify ownership when creating CoLists.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const colleagueAccount = await createJazzTestAccount();
const Task = co.map({
  title: z.string(),
  status: z.string(),
});

// ---cut---
// Create with shared ownership
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

const teamList = co.list(Task).create([], { owner: teamGroup });
```
</CodeGroup>

See [Groups as permission scopes](/docs/groups/intro) for more information on how to use groups to control access to CoLists.

## Reading from CoLists

CoLists support standard array access patterns:

<CodeGroup>
```ts twoslash


const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});

const ListOfTasks = co.list(Task);

const tasks = ListOfTasks.create([
  Task.create({ title: "Prepare soil beds", status: "todo" }),
  Task.create({ title: "Order compost", status: "todo" }),
]);
// ---cut---
// Access by index
const firstTask = tasks[0];
console.log(firstTask.title);  // "Prepare soil beds"

// Get list length
console.log(tasks.length);     // 2

// Iteration
tasks.forEach(task => {
  console.log(task.title);
  // "Prepare soil beds"
  // "Order compost"
});

// Array methods
const todoTasks = tasks.filter(task => task.status === "todo");
console.log(todoTasks.length); // 1
```
</CodeGroup>

## Updating CoLists

Update `CoList`s just like you would JavaScript arrays:

<CodeGroup>
```ts twoslash

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});

const ListOfTasks = co.list(Task);

const ListOfResources = co.list(z.string());

const resources = ListOfResources.create([]);
const tasks = ListOfTasks.create([]);

// ---cut---
// Add items
resources.push("Tomatoes");       // Add to end
resources.unshift("Lettuce");     // Add to beginning
tasks.push(Task.create({          // Add complex items
  title: "Install irrigation",
  status: "todo"
}));

// Replace items
resources[0] = "Cucumber";           // Replace by index

// Modify nested items
tasks[0].status = "complete";        // Update properties of references
```
</CodeGroup>

### Deleting Items

Remove specific items by index with `splice`, or remove the first or last item with `pop` or `shift`:

<CodeGroup>
```ts twoslash

const ListOfResources = co.list(z.string());

const resources = ListOfResources.create([
  "Tomatoes",
  "Cucumber",
  "Peppers",
]);

// ---cut---
// Remove 2 items starting at index 1
resources.splice(1, 2);
console.log(resources);              // ["Cucumber", "Peppers"]

// Remove a single item at index 0
resources.splice(0, 1);
console.log(resources);              // ["Peppers"]

// Remove items
const lastItem = resources.pop();    // Remove and return last item
resources.shift();                   // Remove first item
```
</CodeGroup>

### Array Methods

`CoList`s support the standard JavaScript array methods you already know:

<CodeGroup>
```ts twoslash

const ListOfResources = co.list(z.string());

const resources = ListOfResources.create([]);

// ---cut---
// Add multiple items at once
resources.push("Tomatoes", "Basil", "Peppers");

// Find items
const basil = resources.find(r => r === "Basil");

// Filter (returns regular array, not a CoList)
const tItems = resources.filter(r => r.startsWith("T"));
console.log(tItems); // ["Tomatoes"]

// Sort (modifies the CoList in-place)
resources.sort();
console.log(resources); // ["Basil", "Peppers", "Tomatoes"]
```
</CodeGroup>

### Type Safety

CoLists maintain type safety for their items:

<CodeGroup>
```ts twoslash

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});

const ListOfTasks = co.list(Task);
const ListOfResources = co.list(z.string());

const resources = ListOfResources.create([]);
const tasks = ListOfTasks.create([]);
// ---cut---
// TypeScript catches type errors
resources.push("Carrots");        // ✓ Valid string
// @errors: 2345
resources.push(42);               // ✗ Type error: expected string

// For lists of references
tasks.forEach(task => {
  console.log(task.title);        // TypeScript knows task has title
});
```
</CodeGroup>
## Best Practices

### Common Patterns

#### List Rendering

CoLists work well with UI rendering libraries:

<CodeGroup>
```tsx twoslash

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});

// ---cut---
const ListOfTasks = co.list(Task);

// React example
function TaskList({ tasks }: { tasks: Loaded<typeof ListOfTasks> }) {
  return  (
   <ul>
     {tasks.map(task => (
       task ? (
        <li key={task.id}>
          {task.title} - {task.status}
        </li>
      ): null
     ))}
   </ul>
  );
}
```
</CodeGroup>

#### Managing Relations

CoLists can be used to create one-to-many relationships:

<CodeGroup>
```ts twoslash

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),

  get project(): z.ZodOptional<typeof Project> {
    return z.optional(Project);
  }
});

const ListOfTasks = co.list(Task);

const Project = co.map({
  name: z.string(),

  get tasks(): CoListSchema<typeof Task> {
    return ListOfTasks;
  }
});

const project = Project.create(
  {
    name: "Garden Project",
    tasks: ListOfTasks.create([]),
  },
);

const task = Task.create({
  title: "Plant seedlings",
  status: "todo",
  project: project, // Add a reference to the project
});

// Add a task to a garden project
project.tasks.push(task);

// Access the project from the task
console.log(task.project); // { name: "Garden Project", tasks: [task] }
```
</CodeGroup>



#### CoFeeds

# CoFeeds

CoFeeds are append-only data structures that track entries from different user sessions and accounts. Unlike other CoValues where everyone edits the same data, CoFeeds maintain separate streams for each session.

Each account can have multiple sessions (different browser tabs, devices, or app instances), making CoFeeds ideal for building features like activity logs, presence indicators, and notification systems.

The following examples demonstrate a practical use of CoFeeds:
- [Multi-cursors](https://github.com/garden-co/jazz/tree/main/examples/multi-cursors) - track user presence on a canvas with multiple cursors and out of bounds indicators
- [Reactions](https://github.com/garden-co/jazz/tree/main/examples/reactions) - store per-user emoji reaction using a CoFeed

## Creating CoFeeds

CoFeeds are defined by specifying the type of items they'll contain, similar to how you define CoLists:

<CodeGroup>
```ts twoslash
// ---cut---
// Define a schema for feed items
const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

// Define a feed of garden activities
const ActivityFeed = co.feed(Activity);

// Create a feed instance
const activityFeed = ActivityFeed.create([]);
```
</CodeGroup>

### Ownership

Like other CoValues, you can specify ownership when creating CoFeeds.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const colleagueAccount = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);

// ---cut---
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

const teamFeed = ActivityFeed.create([], { owner: teamGroup });
```
</CodeGroup>

 See [Groups as permission scopes](/docs/groups/intro) for more information on how to use groups to control access to CoFeeds.

## Reading from CoFeeds

Since CoFeeds are made of entries from users over multiple sessions, you can access entries in different ways - from a specific user's session or from their account as a whole.

### Per-Session Access

To retrieve entries from a session:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);
const sessionId = `${me.id}_session_z1` as SessionID;

// ---cut---
// Get the feed for a specific session
const sessionFeed = activityFeed.perSession[sessionId];

// Latest entry from a session
console.log(sessionFeed?.value?.action); // "watering"
```
</CodeGroup>

For convenience, you can also access the latest entry from the current session with `inCurrentSession`:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);
const sessionId = `${me.id}_session_z1` as SessionID;

// ---cut---
// Get the feed for the current session
const currentSessionFeed = activityFeed.inCurrentSession;

// Latest entry from the current session
console.log(currentSessionFeed?.value?.action); // "harvesting"
```
</CodeGroup>

### Per-Account Access

To retrieve entries from a specific account (with entries from all sessions combined) use `perAccount`:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);
const accountId = me.id;

// ---cut---
// Get the feed for a specific account
const accountFeed = activityFeed.perAccount[accountId];

// Latest entry from the account
console.log(accountFeed.value?.action); // "watering"
```
</CodeGroup>

For convenience, you can also access the latest entry from the current account with `byMe`:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);
const accountId = me.id;

// ---cut---
// Get the feed for the current account
const myLatestEntry = activityFeed.byMe;

// Latest entry from the current account
console.log(myLatestEntry?.value?.action); // "harvesting"
```
</CodeGroup>

### Feed Entries

#### All Entries

To retrieve all entries from a CoFeed:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);
const accountId = me.id;
const sessionId = `${me.id}_session_z1` as SessionID;

// ---cut---
// Get the feeds for a specific account and session
const accountFeed = activityFeed.perAccount[accountId];
const sessionFeed = activityFeed.perSession[sessionId];

// Iterate over all entries from the account
for (const entry of accountFeed.all) {
  console.log(entry.value);
}

// Iterate over all entries from the session
for (const entry of sessionFeed.all) {
  console.log(entry.value);
}
```
</CodeGroup>

#### Latest Entry

To retrieve the latest entry from a CoFeed, ie. the last update:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);

// ---cut---
// Get the latest entry from the current account
const latestEntry = activityFeed.byMe;

console.log(`My last action was ${latestEntry?.value?.action}`);
  // "My last action was harvesting"

// Get the latest entry from each account
const latestEntriesByAccount = Object.values(activityFeed.perAccount).map(entry => ({
  accountName: entry.by?.profile?.name,
  value: entry.value,
}));
```
</CodeGroup>

## Writing to CoFeeds

CoFeeds are append-only; you can add new items, but not modify existing ones. This creates a chronological record of events or activities.

### Adding Items

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);

// ---cut---
// Log a new activity
activityFeed.push(Activity.create({
  timestamp: new Date(),
  action: "watering",
  notes: "Extra water for new seedlings"
}));
```
</CodeGroup>

Each item is automatically associated with the current user's session. You don't need to specify which session the item belongs to - Jazz handles this automatically.

### Understanding Session Context

Each entry is automatically added to the current session's feed. When a user has multiple open sessions (like both a mobile app and web browser), each session creates its own separate entries:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const fromMobileFeed = ActivityFeed.create([]);
const fromBrowserFeed = ActivityFeed.create([]);

// ---cut---
// On mobile device:
fromMobileFeed.push(Activity.create({
  timestamp: new Date(),
  action: "harvesting",
  notes: "Vegetable patch"
}));

// On web browser (same user):
fromBrowserFeed.push(Activity.create({
  timestamp: new Date(),
  action: "planting",
  notes: "Flower bed"
}));

// These are separate entries in the same feed, from the same account
```
</CodeGroup>

## Metadata

CoFeeds support metadata, which is useful for tracking information about the feed itself.

### By

The `by` property is the account that made the entry.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);
const accountId = me.id;

// ---cut---
const accountFeed = activityFeed.perAccount[accountId];

// Get the account that made the last entry
console.log(accountFeed?.by);
```
</CodeGroup>

### MadeAt

The `madeAt` property is a timestamp of when the entry was added to the feed.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});

const ActivityFeed = co.feed(Activity);
const activityFeed = ActivityFeed.create([]);
const accountId = me.id;

// ---cut---
const accountFeed = activityFeed.perAccount[accountId];

// Get the timestamp of the last update
console.log(accountFeed?.madeAt);

// Get the timestamp of each entry
for (const entry of accountFeed.all) {
  console.log(entry.madeAt);
}
```
</CodeGroup>

## Best Practices

### When to Use CoFeeds

- **Use CoFeeds when**:
  - You need to track per-user/per-session data
  - Time-based information matters (activity logs, presence)

- **Consider alternatives when**:
  - Data needs to be collaboratively edited (use CoMaps or CoLists)
  - You need structured relationships (use CoMaps/CoLists with references)



#### CoTexts

# CoTexts

Jazz provides two CoValue types for collaborative text editing, collectively referred to as "CoText" values:

- **co.plainText()** for simple text editing without formatting
- **co.richText()** for rich text with HTML-based formatting (extends co.plainText())

Both types enable real-time collaborative editing of text content while maintaining consistency across multiple users.

**Note:** If you're looking for a quick way to add rich text editing to your app, check out [jazz-tools/prosemirror](#using-rich-text-with-prosemirror).

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

// ---cut---
const note = co.plainText().create("Meeting notes", { owner: me });

// Update the text
note.applyDiff("Meeting notes for Tuesday");

console.log(note.toString());  // "Meeting notes for Tuesday"
```
</CodeGroup>

For a full example of CoTexts in action, see [our Richtext example app](https://github.com/garden-co/jazz/tree/main/examples/richtext), which shows plain text and rich text editing.

## co.plainText() vs z.string()

While `z.string()` is perfect for simple text fields, `co.plainText()` is the right choice when you need:

- Frequent text edits that aren't just replacing the whole field
- Fine-grained control over text edits (inserting, deleting at specific positions)
- Multiple users editing the same text simultaneously
- Character-by-character collaboration
- Efficient merging of concurrent changes

Both support real-time updates, but `co.plainText()` provides specialized tools for collaborative editing scenarios.

## Creating CoText Values

CoText values are typically used as fields in your schemas:

<CodeGroup>
```ts twoslash

// ---cut---
const Profile = co.profile({
  name: z.string(),
  bio: co.plainText(),         // Plain text field
  description: co.richText(),  // Rich text with formatting
});
```
</CodeGroup>

Create a CoText value with a simple string:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

// ---cut---
// Create plaintext with default ownership (current user)
const note = co.plainText().create("Meeting notes", { owner: me });

// Create rich text with HTML content
const document = co.richText().create("<p>Project <strong>overview</strong></p>",
  { owner: me }
);
```
</CodeGroup>

### Ownership

Like other CoValues, you can specify ownership when creating CoTexts.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const colleagueAccount = await createJazzTestAccount();

// ---cut---
// Create with shared ownership
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

const teamNote = co.plainText().create("Team updates", { owner: teamGroup });
```
</CodeGroup>

See [Groups as permission scopes](/docs/groups/intro) for more information on how to use groups to control access to CoText values.

## Reading Text

CoText values work similarly to JavaScript strings:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const note = co.plainText().create("Meeting notes", { owner: me });

// ---cut---
// Get the text content
console.log(note.toString());  // "Meeting notes"
console.log(`${note}`);    // "Meeting notes"

// Check the text length
console.log(note.length);      // 14
```
</CodeGroup>

<ContentByFramework framework="react">
When using CoTexts in JSX, you can read the text directly:
<CodeGroup>
```tsx twoslash
const me = await createJazzTestAccount();
const note = co.plainText().create("Meeting notes", { owner: me });

// ---cut---
<>
  <p>{note.toString()}</p>
  <p>{note}</p>
</>
```
</CodeGroup>
</ContentByFramework>

## Making Edits

Insert and delete text with intuitive methods:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const note = co.plainText().create("Meeting notes", { owner: me });

// ---cut---
// Insert text at a specific position
note.insertBefore(8, "weekly ");  // "Meeting weekly notes"

// Insert after a position
note.insertAfter(21, " for Monday");  // "Meeting weekly notes for Monday"

// Delete a range of text
note.deleteRange({ from: 8, to: 15 });  // "Meeting notes for Monday"

// Apply a diff to update the entire text
note.applyDiff("Team meeting notes for Tuesday");
```
</CodeGroup>

### Applying Diffs

Use `applyDiff` to efficiently update text with minimal changes:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

// ---cut---
// Original text: "Team status update"
const minutes = co.plainText().create("Team status update", { owner: me });

// Replace the entire text with a new version
minutes.applyDiff("Weekly team status update for Project X");

// Make partial changes
let text = minutes.toString();
text = text.replace("Weekly", "Monday");
minutes.applyDiff(text);  // Efficiently updates only what changed
```
</CodeGroup>

Perfect for handling user input in form controls:

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash
const me = await createJazzTestAccount();

// ---cut---
function TextEditor({ textId }: { textId: string }) {
  const note = useCoState(co.plainText(), textId);

  return (
    note && <textarea
      value={note.toString()}
      onChange={(e) => {
        // Efficiently update only what the user changed
        note.applyDiff(e.target.value);
      }}
    />
  );
}
```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework="vanilla">
<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();

// ---cut---
const note = co.plainText().create("", { owner: me });

// Create and set up the textarea
const textarea = document.createElement('textarea');
textarea.value = note.toString();

// Add event listener for changes
textarea.addEventListener('input', (e: Event) => {
  const target = e.target as HTMLTextAreaElement;
  // Efficiently update only what the user changed
  note.applyDiff(target.value);
});

// Add the textarea to the document
document.body.appendChild(textarea);
```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework="vue">
<CodeGroup>
```vue twoslash
<script setup lang="ts">

const note = ref(null);
const textContent = ref("");

onMounted(async () => {
  const me = await createJazzTestAccount();
  note.value = co.plainText().create("", { owner: me });
  textContent.value = note.value.toString();
});

function updateText(e) {
  if (note.value) {
    note.value.applyDiff(e.target.value);
    textContent.value = note.value.toString();
  }
}
</script>

<template>
  <textarea
    :value="textContent"
    @input="updateText"
  />
</template>
```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework="svelte">
<CodeGroup>
```svelte twoslash
<script lang="ts">
const me = await createJazzTestAccount();

const note = co.plainText().create("", { owner: me });
</script>

<textarea
  value={note.toString()}
  oninput={e => note.applyDiff(e.target.value)}
/>
```
</CodeGroup>
</ContentByFramework>

## Using Rich Text with ProseMirror

Jazz provides a dedicated plugin for integrating co.richText() with the popular ProseMirror editor. This plugin, [`jazz-tools/prosemirror`](https://www.npmjs.com/package/jazz-tools/prosemirror), enables bidirectional synchronization between your co.richText() instances and ProseMirror editors.

### ProseMirror Plugin Features

- **Bidirectional Sync**: Changes in the editor automatically update the co.richText() and vice versa
- **Real-time Collaboration**: Multiple users can edit the same document simultaneously
- **HTML Conversion**: Automatically converts between HTML (used by co.richText()) and ProseMirror's document model

### Installation

<CodeGroup>
```bash
pnpm add jazz-tools/prosemirror \
  prosemirror-view \
  prosemirror-state \
  prosemirror-schema-basic
```
</CodeGroup>

### Integration

<ContentByFramework framework="react-native">
We don't currently have a React Native-specific example, but you need help you can [request one](https://github.com/garden-co/jazz/issues/new), or ask on [Discord](https://discord.gg/utDMjHYg42).
</ContentByFramework>

<ContentByFramework framework="react-native-expo">
We don't currently have a React Native Expo-specific example, but you need help please [request one](https://github.com/garden-co/jazz/issues/new), or ask on [Discord](https://discord.gg/utDMjHYg42).
</ContentByFramework>

<ContentByFramework framework={["react", "react-native", "react-native-expo"]}>
For use with React:
<CodeGroup>
```tsx twoslash
const JazzProfile = co.profile({
  bio: co.richText(),
});

const JazzAccount = co.account({
  profile: JazzProfile,
  root: co.map({})
});

// ---cut---
// RichTextEditor.tsx

function RichTextEditor() {
  const { me } = useAccount(JazzAccount, { resolve: { profile: true } });
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!me?.profile.bio || !editorRef.current) return;

    // Create the Jazz plugin for ProseMirror
    // Providing a co.richText() instance to the plugin to automatically sync changes
    const jazzPlugin = createJazzPlugin(me.profile.bio); // [!code ++]

    // Set up ProseMirror with the Jazz plugin
    if (!viewRef.current) {
      viewRef.current = new EditorView(editorRef.current, {
        state: EditorState.create({
          schema,
          plugins: [
            ...exampleSetup({ schema }),
            jazzPlugin, // [!code ++]
          ],
        }),
      });
    }

    return () => {
      if (viewRef.current) {
        viewRef.current.destroy();
        viewRef.current = null;
      }
    };
  }, [me?.profile.bio?.id]);

  if (!me) return null;

  return (
    <div className="border rounded">
      <div ref={editorRef} className="p-2" />
    </div>
  );
}
```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework="svelte">
We don't currently have a Svelte-specific example, but you need help you can [request one](https://github.com/garden-co/jazz/issues/new), or ask on [Discord](https://discord.gg/utDMjHYg42).
</ContentByFramework>

<ContentByFramework framework="vue">
We don't currently have a Vue-specific example, but you need help you can [request one](https://github.com/garden-co/jazz/issues/new), or ask on [Discord](https://discord.gg/utDMjHYg42).
</ContentByFramework>

<ContentByFramework framework={["vanilla", "svelte", "vue", "react-native", "react-native-expo"]}>
For use without a framework:
<CodeGroup>
```js twoslash

function setupRichTextEditor(coRichText, container) {
  // Create the Jazz plugin for ProseMirror
  // Providing a co.richText() instance to the plugin to automatically sync changes
  const jazzPlugin = createJazzPlugin(coRichText); // [!code ++]

  // Set up ProseMirror with Jazz plugin
  const view = new EditorView(container, {
    state: EditorState.create({
      schema,
      plugins: [
        ...exampleSetup({ schema }),
        jazzPlugin, // [!code ++]
      ],
    }),
  });

  // Return cleanup function
  return () => {
    view.destroy();
  };
}

// Usage
const document = co.richText().create("<p>Initial content</p>", { owner: me });
const editorContainer = document.getElementById("editor");
const cleanup = setupRichTextEditor(document, editorContainer);

// Later when done with the editor
cleanup();
```
</CodeGroup>
</ContentByFramework>



#### FileStreams

# FileStreams

FileStreams handle binary data in Jazz applications - think documents, audio files, and other non-text content. They're essentially collaborative versions of `Blob`s that sync automatically across devices.

Use FileStreams when you need to:
- Distribute documents across devices
- Store audio or video files
- Sync any binary data between users

**Note:** For images specifically, Jazz provides the higher-level `ImageDefinition` abstraction which manages multiple image resolutions - see the [ImageDefinition documentation](/docs/using-covalues/imagedef) for details.

FileStreams provide automatic chunking when using the `createFromBlob` method, track upload progress, and handle MIME types and metadata.

In your schema, reference FileStreams like any other CoValue:

<CodeGroup>
```ts twoslash
// schema.ts

const Document = co.map({
  title: z.string(),
  file: co.fileStream(),  // Store a document file
});
```
</CodeGroup>

## Creating FileStreams

There are two main ways to create FileStreams: creating empty ones for manual data population or creating directly from existing files or blobs.

### Creating from Blobs and Files

For files from input elements or drag-and-drop interfaces, use `createFromBlob`:

<CodeGroup>
```ts twoslash
const myGroup = Group.create();
const progressBar: HTMLElement = document.querySelector('.progress-bar')!;

// ---cut---
// From a file input
const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement;

fileInput.addEventListener('change', async () => {
  const file = fileInput.files?.[0];
  if (!file) return;

  // Create FileStream from user-selected file
  const fileStream = await co.fileStream().createFromBlob(file, { owner: myGroup });

  // Or with progress tracking for better UX
  const fileWithProgress = await co.fileStream().createFromBlob(file, {
    onProgress: (progress) => {
      // progress is a value between 0 and 1
      const percent = Math.round(progress * 100);
      console.log(`Upload progress: ${percent}%`);
      progressBar.style.width = `${percent}%`;
    },
    owner: myGroup
  });
});
```
</CodeGroup>

### Creating Empty FileStreams

Create an empty FileStream when you want to manually [add binary data in chunks](/docs/using-covalues/filestreams#writing-to-filestreams):

<CodeGroup>
```ts twoslash
const myGroup = Group.create();
// ---cut---
// Create a new empty FileStream
const fileStream = FileStream.create({ owner: myGroup } );
```
</CodeGroup>

### Ownership

Like other CoValues, you can specify ownership when creating FileStreams.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const colleagueAccount = await createJazzTestAccount();

// ---cut---
// Create a team group
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

// Create a FileStream with shared ownership
const teamFileStream = FileStream.create({ owner: teamGroup });
```
</CodeGroup>

 See [Groups as permission scopes](/docs/groups/intro) for more information on how to use groups to control access to FileStreams.

## Reading from FileStreams

`FileStream`s provide several ways to access their binary content, from raw chunks to convenient Blob objects.

### Getting Raw Data Chunks

To access the raw binary data and metadata:

<CodeGroup>
```ts twoslash
const fileStream = FileStream.create();
// ---cut---
// Get all chunks and metadata
const fileData = fileStream.getChunks();

if (fileData) {
  console.log(`MIME type: ${fileData.mimeType}`);
  console.log(`Total size: ${fileData.totalSizeBytes} bytes`);
  console.log(`File name: ${fileData.fileName}`);
  console.log(`Is complete: ${fileData.finished}`);

  // Access raw binary chunks
  for (const chunk of fileData.chunks) {
    // Each chunk is a Uint8Array
    console.log(`Chunk size: ${chunk.length} bytes`);
  }
}
```
</CodeGroup>

By default, `getChunks()` only returns data for completely synced `FileStream`s. To start using chunks from a `FileStream` that's currently still being synced use the `allowUnfinished` option:

<CodeGroup>
```ts twoslash
const fileStream = FileStream.create();
// ---cut---
// Get data even if the stream isn't complete
const partialData = fileStream.getChunks({ allowUnfinished: true });
```
</CodeGroup>

### Converting to Blobs

For easier integration with web APIs, convert to a `Blob`:

<CodeGroup>
```ts twoslash
const fileStream = FileStream.create();
// ---cut---
// Convert to a Blob
const blob = fileStream.toBlob();

// Get the filename from the metadata
const filename = fileStream.getChunks()?.fileName;

if (blob) {
  // Use with URL.createObjectURL
  const url = URL.createObjectURL(blob);

  // Create a download link
  const link = document.createElement('a');
  link.href = url;
  link.download = filename || 'document.pdf';
  link.click();

  // Clean up when done
  URL.revokeObjectURL(url);
}
```
</CodeGroup>

### Loading FileStreams as Blobs

You can directly load a `FileStream` as a `Blob` when you only have its ID:

<CodeGroup>
```ts twoslash
const fileStreamId = "co_z123" as ID<FileStream>;
// ---cut---
// Load directly as a Blob when you have an ID
const blob = await FileStream.loadAsBlob(fileStreamId);

// By default, waits for complete uploads
// For in-progress uploads:
const partialBlob = await FileStream.loadAsBlob(fileStreamId, {
  allowUnfinished: true,
});
```
</CodeGroup>

### Checking Completion Status

Check if a `FileStream` is fully synced:

<CodeGroup>
```ts twoslash
const fileStream = FileStream.create();
// ---cut---
if (fileStream.isBinaryStreamEnded()) {
  console.log('File is completely synced');
} else {
  console.log('File upload is still in progress');
}
```
</CodeGroup>

## Writing to FileStreams

When creating a `FileStream` manually (not using `createFromBlob`), you need to manage the upload process yourself. This gives you more control over chunking and progress tracking.

### The Upload Lifecycle

`FileStream` uploads follow a three-stage process:

1. **Start** - Initialize with metadata
2. **Push** - Send one or more chunks of data
3. **End** - Mark the stream as complete

### Starting a `FileStream`

Begin by providing metadata about the file:

<CodeGroup>
```ts twoslash
const myGroup = Group.create();
// ---cut---
// Create an empty FileStream
const fileStream = FileStream.create({ owner: myGroup });

// Initialize with metadata
fileStream.start({
  mimeType: 'application/pdf',      // MIME type (required)
  totalSizeBytes: 1024 * 1024 * 2,  // Size in bytes (if known)
  fileName: 'document.pdf'          // Original filename (optional)
});
```
</CodeGroup>

### Pushing Data

Add binary data in chunks - this helps with large files and progress tracking:

<CodeGroup>
```ts twoslash
const fileStream = FileStream.create();
const file = [0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64]; // "Hello World" in ASCII
const bytes = new Uint8Array(file);
const arrayBuffer = bytes.buffer;

// ---cut---
const data = new Uint8Array(arrayBuffer);

// For large files, break into chunks (e.g., 100KB each)
const chunkSize = 1024 * 100;
for (let i = 0; i < data.length; i += chunkSize) {
  // Create a slice of the data
  const chunk = data.slice(i, i + chunkSize);

  // Push chunk to the FileStream
  fileStream.push(chunk);

  // Track progress
  const progress = Math.min(100, Math.round((i + chunk.length) * 100 / data.length));
  console.log(`Upload progress: ${progress}%`);
}
```
</CodeGroup>

### Completing the Upload

Once all chunks are pushed, mark the `FileStream` as complete:

<CodeGroup>
```ts twoslash
const fileStream = FileStream.create();
// ---cut---
// Finalize the upload
fileStream.end();

console.log('Upload complete!');
```
</CodeGroup>

## Subscribing to `FileStream`s

Like other CoValues, you can subscribe to `FileStream`s to get notified of changes as they happen. This is especially useful for tracking upload progress when someone else is uploading a file.

### Loading by ID

Load a `FileStream` when you have its ID:

<CodeGroup>
```ts twoslash
const fileStreamId = "co_z123";
// ---cut---
// Load a FileStream by ID
const fileStream = await FileStream.load(fileStreamId);

if (fileStream) {
  console.log('FileStream loaded successfully');

  // Check if it's complete
  if (fileStream.isBinaryStreamEnded()) {
    // Process the completed file
    const blob = fileStream.toBlob();
  }
}
```
</CodeGroup>

### Subscribing to Changes

Subscribe to a `FileStream` to be notified when chunks are added or when the upload is complete:

<CodeGroup>
```ts twoslash
const fileStreamId = "co_z123";
// ---cut---
// Subscribe to a FileStream by ID
const unsubscribe = FileStream.subscribe(fileStreamId, (fileStream: FileStream) => {
  // Called whenever the FileStream changes
  console.log('FileStream updated');

  // Get current status
  const chunks = fileStream.getChunks({ allowUnfinished: true });
  if (chunks) {
    const uploadedBytes = chunks.chunks.reduce((sum: number, chunk: Uint8Array) => sum + chunk.length, 0);
    const totalBytes = chunks.totalSizeBytes || 1;
    const progress = Math.min(100, Math.round(uploadedBytes * 100 / totalBytes));

    console.log(`Upload progress: ${progress}%`);

    if (fileStream.isBinaryStreamEnded()) {
      console.log('Upload complete!');
      // Now safe to use the file
      const blob = fileStream.toBlob();

      // Clean up the subscription if we're done
      unsubscribe();
    }
  }
});
```
</CodeGroup>

### Waiting for Upload Completion

If you need to wait for a `FileStream` to be fully synchronized across devices:

<CodeGroup>
```ts twoslash
const fileStream = FileStream.create();
// ---cut---
// Wait for the FileStream to be fully synced
await fileStream.waitForSync({
  timeout: 5000  // Optional timeout in ms
});

console.log('FileStream is now synced to all connected devices');
```
</CodeGroup>

This is useful when you need to ensure that a file is available to other users before proceeding with an operation.



#### ImageDefinition

### react-native-expo Implementation

# ImageDefinition

`ImageDefinition` is a specialized CoValue designed specifically for managing images in Jazz. It extends beyond basic file storage by supporting multiple resolutions of the same image, optimized for mobile devices.

**Note**: This guide applies to both Expo and framework-less React Native implementations. The functionality described here is identical regardless of which implementation you're using, though you'll need to import from the appropriate package (`jazz-expo` or `jazz-react-native`).

Jazz offers several tools to work with images in React Native:
- [`createImage()`](#creating-images) - function to create an `ImageDefinition` from a base64 image data URI
- [`ProgressiveImg`](#displaying-images-with-progressiveimg) - React component to display an image with progressive loading
- [`useProgressiveImg`](#using-useprogressiveimg-hook) - React hook to load an image in your own component

For examples of use, see our example apps:
- [React Native Chat](https://github.com/gardencmp/jazz/tree/main/examples/chat-rn) (Framework-less implementation)
- [React Native Expo Chat](https://github.com/gardencmp/jazz/tree/main/examples/chat-rn-expo) (Expo implementation)
- [React Native Expo Clerk Chat](https://github.com/gardencmp/jazz/tree/main/examples/chat-rn-expo-clerk) (Expo implementation with Clerk)

## Creating Images

The easiest way to create and use images in your Jazz application is with the `createImage()` function:

<CodeGroup>
```tsx

async function handleImagePicker() {
  try {
    // Launch the image picker
    const result = await ImagePicker.launchImageLibraryAsync({
      mediaTypes: ImagePicker.MediaTypeOptions.Images,
      base64: true,
      quality: 1,
    });

    if (!result.canceled) {
      const base64Uri = `data:image/jpeg;base64,${result.assets[0].base64}`;

      // Creates ImageDefinition with multiple resolutions automatically
      const image = await createImage(base64Uri, {
        owner: me.profile._owner,
        maxSize: 2048, // Optional: limit maximum resolution
      });

      // Store the image
      me.profile.image = image;
    }
  } catch (error) {
    console.error("Error creating image:", error);
  }
}
```
</CodeGroup>

The `createImage()` function:
- Creates an `ImageDefinition` with the right properties
- Generates a small placeholder for immediate display
- Creates multiple resolution variants of your image
- Returns the created `ImageDefinition`

### Configuration Options

You can configure `createImage()` with additional options:

<CodeGroup>
```tsx
// Configuration options
const options = {
  owner: me,                // Owner for access control
  maxSize: 1024             // Maximum resolution to generate
};

// Setting maxSize controls which resolutions are generated:
// 256: Only creates the smallest resolution (256px on longest side)
// 1024: Creates 256px and 1024px resolutions
// 2048: Creates 256px, 1024px, and 2048px resolutions
// undefined: Creates all resolutions including the original size

const image = await createImage(base64Uri, options);
```
</CodeGroup>

## Displaying Images with `ProgressiveImg`

For a complete progressive loading experience, use the `ProgressiveImg` component:

<CodeGroup>
```tsx

function GalleryView({ image }) {
  return (
    <ProgressiveImg
      image={image}  // The image definition to load
      targetWidth={800} //  Looks for the best available resolution for a 800px image
    >
      {({ src }) => (
        <Image
          source={{ uri: src }}
          style={styles.galleryImage}
          resizeMode="cover"
        />
      )}
    </ProgressiveImg>
  );
}

const styles = StyleSheet.create({
  galleryImage: {
    width: '100%',
    height: 200,
    borderRadius: 8,
  }
});
```
</CodeGroup>

The `ProgressiveImg` component handles:
- Showing a placeholder while loading
- Automatically selecting the appropriate resolution
- Progressive enhancement as higher resolutions become available
- Cleaning up resources when unmounted

## Using `useProgressiveImg` Hook

For more control over image loading, you can implement your own progressive image component:

<CodeGroup>
```tsx

function CustomImageComponent({ image }) {
  const {
    src,         // Data URI containing the image data as a base64 string,
                 // or a placeholder image URI
    res,         // The current resolution
    originalSize // The original size of the image
  } = useProgressiveImg({
    image: image,  // The image definition to load
    targetWidth: 800  // Limit to resolutions up to 800px wide
  });

  // When image is not available yet
  if (!src) {
    return (
      <View style={{ height: 200, justifyContent: 'center', alignItems: 'center', backgroundColor: '#f0f0f0' }}>
        <ActivityIndicator size="small" color="#0000ff" />
        <Text style={{ marginTop: 10 }}>Loading image...</Text>
      </View>
    );
  }

  // When using placeholder
  if (res === "placeholder") {
    return (
      <View style={{ position: 'relative' }}>
        <Image
          source={{ uri: src }}
          style={{ width: '100%', height: 200, opacity: 0.7 }}
          resizeMode="cover"
        />
        <ActivityIndicator
          size="large"
          color="#ffffff"
          style={{ position: 'absolute', top: '50%', left: '50%', marginLeft: -20, marginTop: -20 }}
        />
      </View>
    );
  }

  // Full image display with custom overlay
  return (
    <View style={{ position: 'relative', width: '100%', height: 200 }}>
      <Image
        source={{ uri: src }}
        style={{ width: '100%', height: '100%' }}
        resizeMode="cover"
      />
      <View style={{ position: 'absolute', bottom: 0, left: 0, right: 0, backgroundColor: 'rgba(0,0,0,0.5)', padding: 8 }}>
        <Text style={{ color: 'white' }}>Resolution: {res}</Text>
      </View>
    </View>
  );
}
```
</CodeGroup>

## Understanding ImageDefinition

Behind the scenes, `ImageDefinition` is a specialized CoValue that stores:

- The original image dimensions (`originalSize`)
- An optional placeholder (`placeholderDataURL`) for immediate display
- Multiple resolution variants of the same image as [`FileStream`s](../using-covalues/filestreams)

Each resolution is stored with a key in the format `"widthxheight"` (e.g., `"1920x1080"`, `"800x450"`).

<CodeGroup>
```tsx
// Structure of an ImageDefinition
const image = ImageDefinition.create({
  originalSize: [1920, 1080],
  placeholderDataURL: "data:image/jpeg;base64,/9j/4AAQSkZJRg...",
});

// Accessing the highest available resolution
const highestRes = image.highestResAvailable();
if (highestRes) {
  console.log(`Found resolution: ${highestRes.res}`);
  console.log(`Stream: ${highestRes.stream}`);
}
```
</CodeGroup>

For more details on using `ImageDefinition` directly, see the [VanillaJS docs](/docs/vanilla/using-covalues/imagedef).

### Fallback Behavior

`highestResAvailable` returns the largest resolution that fits your constraints. If a resolution has incomplete data, it falls back to the next available lower resolution.

<CodeGroup>
```tsx
const image = ImageDefinition.create({
  originalSize: [1920, 1080],
});

image["1920x1080"] = FileStream.create(); // Empty image upload
image["800x450"] = await FileStream.createFromBlob(mediumSizeBlob);

const highestRes = image.highestResAvailable();
console.log(highestRes.res); // 800x450
```
</CodeGroup>

---

### react-native Implementation

# ImageDefinition

`ImageDefinition` is a specialized CoValue designed specifically for managing images in Jazz. It extends beyond basic file storage by supporting multiple resolutions of the same image, optimized for mobile devices.

**Note**: This guide applies to both Expo and framework-less React Native implementations. The functionality described here is identical regardless of which implementation you're using, though you'll need to import from the appropriate package (`jazz-expo` or `jazz-react-native`).

Jazz offers several tools to work with images in React Native:
- [`createImage()`](#creating-images) - function to create an `ImageDefinition` from a base64 image data URI
- [`ProgressiveImg`](#displaying-images-with-progressiveimg) - React component to display an image with progressive loading
- [`useProgressiveImg`](#using-useprogressiveimg-hook) - React hook to load an image in your own component

For examples of use, see our example apps:
- [React Native Chat](https://github.com/gardencmp/jazz/tree/main/examples/chat-rn) (Framework-less implementation)
- [React Native Expo Chat](https://github.com/gardencmp/jazz/tree/main/examples/chat-rn-expo) (Expo implementation)
- [React Native Expo Clerk Chat](https://github.com/gardencmp/jazz/tree/main/examples/chat-rn-expo-clerk) (Expo implementation with Clerk)

## Creating Images

The easiest way to create and use images in your Jazz application is with the `createImage()` function:

<CodeGroup>
```tsx

async function handleImagePicker() {
  try {
    // Launch the image picker
    const result = await launchImageLibrary({
      mediaType: 'photo',
      includeBase64: true,
      quality: 1,
    });

    if (!result.canceled) {
      const base64Uri = `data:image/jpeg;base64,${result.assets[0].base64}`;

      // Creates ImageDefinition with multiple resolutions automatically
      const image = await createImage(base64Uri, {
        owner: me.profile._owner,
        maxSize: 2048, // Optional: limit maximum resolution
      });

      // Store the image
      me.profile.image = image;
    }
  } catch (error) {
    console.error("Error creating image:", error);
  }
}
```
</CodeGroup>

The `createImage()` function:
- Creates an `ImageDefinition` with the right properties
- Generates a small placeholder for immediate display
- Creates multiple resolution variants of your image
- Returns the created `ImageDefinition`

### Configuration Options

You can configure `createImage()` with additional options:

<CodeGroup>
```tsx
// Configuration options
const options = {
  owner: me,                // Owner for access control
  maxSize: 1024             // Maximum resolution to generate
};

// Setting maxSize controls which resolutions are generated:
// 256: Only creates the smallest resolution (256px on longest side)
// 1024: Creates 256px and 1024px resolutions
// 2048: Creates 256px, 1024px, and 2048px resolutions
// undefined: Creates all resolutions including the original size

const image = await createImage(base64Uri, options);
```
</CodeGroup>

## Displaying Images with `ProgressiveImg`

For a complete progressive loading experience, use the `ProgressiveImg` component:

<CodeGroup>
```tsx

function GalleryView({ image }) {
  return (
    <ProgressiveImg
      image={image}  // The image definition to load
      targetWidth={800} //  Looks for the best available resolution for a 800px image
    >
      {({ src }) => (
        <Image
          source={{ uri: src }}
          style={styles.galleryImage}
          resizeMode="cover"
        />
      )}
    </ProgressiveImg>
  );
}

const styles = StyleSheet.create({
  galleryImage: {
    width: '100%',
    height: 200,
    borderRadius: 8,
  }
});
```
</CodeGroup>

The `ProgressiveImg` component handles:
- Showing a placeholder while loading
- Automatically selecting the appropriate resolution
- Progressive enhancement as higher resolutions become available
- Cleaning up resources when unmounted

## Using `useProgressiveImg` Hook

For more control over image loading, you can implement your own progressive image component:

<CodeGroup>
```tsx

function CustomImageComponent({ image }) {
  const {
    src,         // Data URI containing the image data as a base64 string,
                 // or a placeholder image URI
    res,         // The current resolution
    originalSize // The original size of the image
  } = useProgressiveImg({
    image: image,  // The image definition to load
    targetWidth: 800  // Limit to resolutions up to 800px wide
  });

  // When image is not available yet
  if (!src) {
    return (
      <View style={{ height: 200, justifyContent: 'center', alignItems: 'center', backgroundColor: '#f0f0f0' }}>
        <ActivityIndicator size="small" color="#0000ff" />
        <Text style={{ marginTop: 10 }}>Loading image...</Text>
      </View>
    );
  }

  // When using placeholder
  if (res === "placeholder") {
    return (
      <View style={{ position: 'relative' }}>
        <Image
          source={{ uri: src }}
          style={{ width: '100%', height: 200, opacity: 0.7 }}
          resizeMode="cover"
        />
        <ActivityIndicator
          size="large"
          color="#ffffff"
          style={{ position: 'absolute', top: '50%', left: '50%', marginLeft: -20, marginTop: -20 }}
        />
      </View>
    );
  }

  // Full image display with custom overlay
  return (
    <View style={{ position: 'relative', width: '100%', height: 200 }}>
      <Image
        source={{ uri: src }}
        style={{ width: '100%', height: '100%' }}
        resizeMode="cover"
      />
      <View style={{ position: 'absolute', bottom: 0, left: 0, right: 0, backgroundColor: 'rgba(0,0,0,0.5)', padding: 8 }}>
        <Text style={{ color: 'white' }}>Resolution: {res}</Text>
      </View>
    </View>
  );
}
```
</CodeGroup>

## Understanding ImageDefinition

Behind the scenes, `ImageDefinition` is a specialized CoValue that stores:

- The original image dimensions (`originalSize`)
- An optional placeholder (`placeholderDataURL`) for immediate display
- Multiple resolution variants of the same image as [`FileStream`s](../using-covalues/filestreams)

Each resolution is stored with a key in the format `"widthxheight"` (e.g., `"1920x1080"`, `"800x450"`).

<CodeGroup>
```tsx
// Structure of an ImageDefinition
const image = ImageDefinition.create({
  originalSize: [1920, 1080],
  placeholderDataURL: "data:image/jpeg;base64,/9j/4AAQSkZJRg...",
});

// Accessing the highest available resolution
const highestRes = image.highestResAvailable();
if (highestRes) {
  console.log(`Found resolution: ${highestRes.res}`);
  console.log(`Stream: ${highestRes.stream}`);
}
```
</CodeGroup>

For more details on using `ImageDefinition` directly, see the [VanillaJS docs](/docs/vanilla/using-covalues/imagedef).

### Fallback Behavior

`highestResAvailable` returns the largest resolution that fits your constraints. If a resolution has incomplete data, it falls back to the next available lower resolution.

<CodeGroup>
```tsx
const image = ImageDefinition.create({
  originalSize: [1920, 1080],
});

image["1920x1080"] = FileStream.create(); // Empty image upload
image["800x450"] = await FileStream.createFromBlob(mediumSizeBlob);

const highestRes = image.highestResAvailable();
console.log(highestRes.res); // 800x450
```
</CodeGroup>

---

### react Implementation

# ImageDefinition

`ImageDefinition` is a specialized CoValue designed specifically for managing images in Jazz applications. It extends beyond basic file storage by supporting multiple resolutions of the same image and progressive loading patterns.

Beyond [`ImageDefinition`](#understanding-imagedefinition), Jazz offers higher-level functions and components that make it easier to use images:
- [`createImage()`](#creating-images) - function to create an `ImageDefinition` from a file
- [`ProgressiveImg`](#displaying-images-with-progressiveimg) - React component to display an image with progressive loading
- [`useProgressiveImg`](#using-useprogressiveimg-hook) - React hook to load an image in your own component

The [Image Upload example](https://github.com/gardencmp/jazz/tree/main/examples/image-upload) demonstrates use of `ProgressiveImg` and `ImageDefinition`.

## Creating Images

The easiest way to create and use images in your Jazz application is with the `createImage()` function:

<CodeGroup>
```ts twoslash

const MyProfile = co.profile({
  name: z.string(),
  image: z.optional(co.image()),
});

const MyAccount = co.account({
  root: co.map({}),
  profile: MyProfile,
});

MyAccount.withMigration((account, creationProps) => {
  if (account.profile === undefined) {
    const profileGroup = Group.create();
    profileGroup.addMember("everyone", "reader");
    account.profile = MyProfile.create(
      {
        name: creationProps?.name ?? "New user",
      },
      profileGroup,
    );
  }
});

const me = await MyAccount.create({});

const myGroup = Group.create();

// ---cut---

// Create an image from a file input
async function handleFileUpload(event: React.ChangeEvent<HTMLInputElement>) {
  const file = event.target.files?.[0];
  if (file) {
    // Creates ImageDefinition with multiple resolutions automatically
    const image = await createImage(file, { owner: myGroup });

    // Store the image in your application data
    me.profile.image = image;
  }
}

```
</CodeGroup>

**Note:** `createImage()` requires a browser environment as it uses browser APIs to process images.

The `createImage()` function:
- Creates an `ImageDefinition` with the right properties
- Generates a small placeholder for immediate display
- Creates multiple resolution variants of your image
- Returns the created `ImageDefinition`

### Configuration Options

You can configure `createImage()` with additional options:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const file = new File([], "test.jpg", { type: "image/jpeg" });
// ---cut---
// Configuration options
const options = {
  owner: me,                // Owner for access control
  maxSize: 1024 as 1024     // Maximum resolution to generate
};

// Setting maxSize controls which resolutions are generated:
// 256: Only creates the smallest resolution (256px on longest side)
// 1024: Creates 256px and 1024px resolutions
// 2048: Creates 256px, 1024px, and 2048px resolutions
// undefined: Creates all resolutions including the original size

const image = await createImage(file, options);
```
</CodeGroup>

### Ownership

Like other CoValues, you can specify ownership when creating image definitions.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const colleagueAccount = await createJazzTestAccount();

const file = new File([], "test.jpg", { type: "image/jpeg" });

// ---cut---
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

// Create an image with shared ownership
const teamImage = await createImage(file, { owner: teamGroup });
```
</CodeGroup>

See [Groups as permission scopes](/docs/groups/intro) for more information on how to use groups to control access to images.

## Displaying Images with `ProgressiveImg`

For a complete progressive loading experience, use the `ProgressiveImg` component:

<CodeGroup>
```tsx twoslash
// ---cut---
const Image = co.image();

function GalleryView({ image }: { image: Loaded<typeof Image> }) {
  return (
    <div className="image-container">
      <ProgressiveImg
        image={image}  // The image definition to load
        targetWidth={800} // Looks for the best available resolution for a 800px image
      >
        {({ src }) => (
          <img 
            src={src} 
            alt="Gallery image" 
            className="gallery-image"
          />
        )}
      </ProgressiveImg>
    </div>
  );
}
```
</CodeGroup>

The `ProgressiveImg` component handles:
- Showing a placeholder while loading
- Automatically selecting the appropriate resolution
- Progressive enhancement as higher resolutions become available
- Cleaning up resources when unmounted

## Using `useProgressiveImg` Hook

For more control over image loading, you can implement your own progressive image component:

<CodeGroup>
```tsx twoslash
const Image = co.image();
// ---cut---

function CustomImageComponent({ image }: { image: Loaded<typeof Image> }) {
  const {
    src,         // Data URI containing the image data as a base64 string,
                 // or a placeholder image URI
    res,         // The current resolution
    originalSize // The original size of the image
  } = useProgressiveImg({
    image: image,  // The image definition to load
    targetWidth: 800  // Limit to resolutions up to 800px wide
  });

  // When image is not available yet
  if (!src) {
    return <div className="image-loading-fallback">Loading image...</div>;
  }
  
  // When image is loading, show a placeholder
  if (res === "placeholder") {
    return <img src={src} alt="Loading..." className="blur-effect" />;
  }

  // Full image display with custom overlay
  return (
    <div className="custom-image-wrapper">
      <img 
        src={src} 
        alt="Custom image" 
        className="custom-image"
      />
      <div className="image-overlay">
        <span className="image-caption">Resolution: {res}</span>
      </div>
    </div>
  );
}
```
</CodeGroup>

## Understanding ImageDefinition

Behind the scenes, `ImageDefinition` is a specialized CoValue that stores:

- The original image dimensions (`originalSize`)
- An optional placeholder (`placeholderDataURL`) for immediate display
- Multiple resolution variants of the same image as [`FileStream`s](../using-covalues/filestreams)

Each resolution is stored with a key in the format `"widthxheight"` (e.g., `"1920x1080"`, `"800x450"`).

<CodeGroup>
```ts twoslash
// ---cut---
// Structure of an ImageDefinition
const image = ImageDefinition.create({
  originalSize: [1920, 1080],
  placeholderDataURL: "data:image/jpeg;base64,/9j/4AAQSkZJRg...",
});

// Accessing the highest available resolution
const highestRes = ImageDefinition.highestResAvailable(image);
if (highestRes) {
  console.log(`Found resolution: ${highestRes.res}`);
  console.log(`Stream: ${highestRes.stream}`);
}
```
</CodeGroup>

For more details on using `ImageDefinition` directly, see the [VanillaJS docs](/docs/vanilla/using-covalues/imagedef).

### Fallback Behavior

`highestResAvailable` returns the largest resolution that fits your constraints. If a resolution has incomplete data, it falls back to the next available lower resolution.

<CodeGroup>
```ts twoslash
const mediumSizeBlob = new Blob([], { type: "image/jpeg" });

// ---cut---
const image = ImageDefinition.create({
  originalSize: [1920, 1080],
});

image["1920x1080"] = FileStream.create(); // Empty image upload
image["800x450"] = await FileStream.createFromBlob(mediumSizeBlob);

const highestRes = ImageDefinition.highestResAvailable(image);
console.log(highestRes?.res); // 800x450
```
</CodeGroup>



#### Subscriptions & Deep Loading

# Subscriptions & Deep Loading

Jazz's Collaborative Values (such as [CoMaps](/docs/using-covalues/comaps) or [CoLists](/docs/using-covalues/colists)) work like reactive state. By subscribing to them, you can react to both local and remote updates. This is the main way to consume data in your application.

Subscriptions also take care of loading CoValues that are not yet loaded locally and can do so *deeply* &mdash; by resolving nested CoValues. To make use of this, we'll show you how to specify the depth of data you need with resolve queries.

With each update you can also handle loading states and inaccessible CoValues.

## Manual subscriptions

You can subscribe to a CoValue from anywhere in your code (if you have its ID) by using `CoValue.subscribe()`.

<ContentByFramework framework="vanilla">
If you're using React in your project, check out our [React hooks](/docs/react/using-covalues/subscription-and-loading#subscription-hooks) which provide a more streamlined experience with automatic subscription management.
</ContentByFramework>

<ContentByFramework framework={["react", "react-native"]}>
**Note:** Unless you're using vanilla JavaScript, this is only used outside of React components - for example in server-side code or in tests. See the section below for convenient subscription *hooks* that you typically use in React.
</ContentByFramework>

<CodeGroup>
```ts twoslash
const taskId = "co_123";
// ---cut-before---
const Task = co.map({
  title: z.string(),
  description: z.string(),
  status: z.literal(["todo", "in-progress", "completed"]),
  assignedTo: z.optional(z.string()),
});

// ...

// Subscribe to a Task by ID
const unsubscribe = Task.subscribe(taskId, {}, (updatedTask) => {
  console.log("Task updated:", updatedTask.title);
  console.log("New status:", updatedTask.status);
});

// Clean up when you're done
unsubscribe();
```
</CodeGroup>

If you already have a CoValue instance, you can subscribe to it by calling its `subscribe` method.

<CodeGroup>
```ts twoslash

const Task = co.map({
  title: z.string(),
  description: z.string(),
  status: z.literal(["todo", "in-progress", "completed"]),
  assignedTo: z.optional(z.string()),
});
const otherProps = {} as any;
// ---cut-before---
const task = Task.create({
  title: "Cut the grass",
  ...otherProps
});

const unsubscribe = task.subscribe((updatedTask) => {
  console.log("Task updated:", updatedTask.title);
});

// Clean up when you're done
unsubscribe();
```
</CodeGroup>

<ContentByFramework framework={["react", "react-native"]}>
## Subscription hooks

### `useCoState`

Jazz provides a `useCoState` hook that provides a convenient way to subscribe to CoValues and handle loading states:

<CodeGroup>
```tsx twoslash

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "completed"]),
});
const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
});
// ---cut-before---

function GardenPlanner({ projectId }: { projectId: string }) {
  // Subscribe to a project and its tasks
  const project = useCoState(Project, projectId, {
    resolve: {
      tasks: { $each: true },
    },
  });

  if (!project) {
    return project === null
      ? "Project not found or not accessible"
      : "Loading project ...";
  }

  return (
    <div>
      <h1>{project.name}</h1>
      <TaskList tasks={project.tasks} />
    </div>
  );
}

function TaskList({ tasks }: { tasks: Loaded<typeof Task>[] }) {
  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>
          <span>{task.title}</span>
          <span>{task.status}</span>
        </li>
      ))}
    </ul>
  );
}
```
</CodeGroup>

The `useCoState` hook handles subscribing when the component mounts and unsubscribing when it unmounts, making it easy to keep your UI in sync with the underlying data.

### `useAccount`

`useAccount` is used to access the current user's account.
You can use this at the top-level of your app to subscribe to the current user's [account profile and root](../schemas/accounts-and-migrations#covalues-as-a-graph-of-data-rooted-in-accounts).

Like `useCoState`, you can specify a resolve query to also subscribe to CoValues referenced in the account profile or root.

<CodeGroup>
```tsx twoslash
const Task = co.map({
  title: z.string(),
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
});

const AccountRoot = co.map({
  myProjects: co.list(Project),
});

const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.profile(),
});

// ---cut-before---

function ProjectList() {
  const { me } = useAccount(MyAppAccount, {
    resolve: {
      profile: true,
      root: {
        myProjects: {
          $each: {
            tasks: true,
          },
        },
      },
    },
  });

  if (!me) {
    return <div>Loading...</div>;
  }

  return (
    <div>
      <h1>{me.profile.name}'s projects</h1>
      <ul>
        {me.root.myProjects.map((project) => (
          <li key={project.id}>
            {project.name} ({project.tasks.length} tasks)
          </li>
        ))}
      </ul>
    </div>
  );
}

```
</CodeGroup>

</ContentByFramework>

## Loading States and Permission Checking

When subscribing to or loading a CoValue, you need to handle three possible states:

- `undefined`: The initial loading state, indicating the value is being fetched
- `null`: The CoValue was not found or is not accessible (e.g., due to permissions)
- `Value`: The successfully loaded CoValue instance

This allows you to handle loading, error, and success states in your application:

<CodeGroup>
```ts twoslash
const Task = co.map({
  title: z.string(),
});

const taskId = "co_123";
// ---cut-before---
Task.subscribe(taskId, {}, (task: Loaded<typeof Task>) => {
  if (task === undefined) {
    console.log("Task is loading...");
  } else if (task === null) {
    console.log("Task not found or not accessible");
  } else {
    console.log("Task loaded:", task.title);
  }
});
```
</CodeGroup>

## Deep Loading

When working with related CoValues (like tasks in a project), you often need to load not just the top-level object but also its nested references. This is especially important when working with [CoMaps](/docs/using-covalues/comaps) that contain references to other CoValues or with [CoLists](/docs/using-covalues/colists) that contain multiple items. Jazz provides a flexible mechanism for specifying exactly how much of the object graph to load.

### Resolve queries

Resolve queries let you declare exactly which references to load and how deep to go using the `resolve` property:

<CodeGroup>
```ts twoslash
const projectId = "co_123";

// ---cut-before---
const TeamMember = co.map({
  name: z.string(),
});

const Task = co.map({
  title: z.string(),
  assignee: z.optional(TeamMember),
  get subtasks(): CoListSchema<typeof Task> { return co.list(Task) },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
  owner: TeamMember,
});

// Load just the project, not its references
const project = await Project.load(projectId);
if (!project) { throw new Error("Project not found or not accessible"); }

// string - primitive fields are always loaded
project.name;
// undefined | null | ListOfTasks - non-requested references might not be loaded, or inaccessible
project.tasks;

// Load the project and shallowly load its list of tasks
const projectWithTasksShallow = await Project.load(projectId, {
  resolve: {
    tasks: true
  }
});
if (!projectWithTasksShallow) { throw new Error("Project or required references not found or not accessible"); }

// ListOfTasks - shallowly loaded
projectWithTasksShallow.tasks;
// number - length of the list
projectWithTasksShallow.tasks.length;
// undefined | null | Task - items might not be loaded, or inaccessible
projectWithTasksShallow.tasks[0];

// Load the project and its tasks
const projectWithTasks = await Project.load(projectId, {
  resolve: {
    tasks: {
      $each: true
    }
  }
});
if (!projectWithTasks) { throw new Error("Project or required references not found or not accessible"); }

// ListOfTasks - fully loaded
projectWithTasks.tasks;
// Task - fully loaded
projectWithTasks.tasks[0];
// string - primitive fields are always loaded
projectWithTasks.tasks[0].title;
// undefined | null | ListOfTasks - subtasks might not be loaded, or inaccessible
projectWithTasks.tasks[0].subtasks;

// Load the project, its tasks, and their subtasks
const projectDeep = await Project.load(projectId, {
  resolve: {
    tasks: {
      $each: {
        subtasks: {
          $each: true
        },
        assignee: true
      }
    }
  }
});
if (!projectDeep) { throw new Error("Project or required references not found or not accessible"); }

// string - primitive fields are always loaded
projectDeep.tasks[0].subtasks[0].title;
// undefined | null | TeamMember - since assignee is optional:
//   TeamMember - set and definitely loaded
//   null - set but unavailable/inaccessible
//   undefined - not set, or loading (in case of subscription)
projectDeep.tasks[0].assignee;
```
</CodeGroup>

The resolve query defines which parts of the graph you want to load, making it intuitive to express complex loading patterns.

### Loading states and permissions

When loading data with references, the load operation will fail if one of the references is unavailable or if the user doesn't have read access to it. Let's explore what happens in various scenarios:

#### Resolved References

When a user tries to load a reference they don't have access to:

<CodeGroup>
```ts twoslash

const TeamMember = co.map({
  name: z.string(),
});

const Task = co.map({
  title: z.string(),
  assignee: z.optional(TeamMember),
  get subtasks(): CoListSchema<typeof Task> { return co.list(Task) },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
  owner: TeamMember,
});

const taskId = "co_123";

// ---cut-before---
// If assignee is not accessible to the user:
const task = await Task.load(taskId, {
  resolve: { assignee: true }
});

task // => null
```
</CodeGroup>
The load operation will fail and return `null` if any requested reference is inaccessible. This maintains data consistency by ensuring all requested references are available before returning the object.

The behavior is the same for optional and required references.

#### List References

When a list contains references to items the user can't access:

<CodeGroup>
```ts twoslash

const TeamMember = co.map({
  name: z.string(),
});

const Task = co.map({
  title: z.string(),
  assignee: z.optional(TeamMember),
  get subtasks(): CoListSchema<typeof Task> { return co.list(Task) },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
  owner: TeamMember,
});

const projectId = "co_123";
// ---cut-before---
// If any item in the list is not accessible:
const project = await Project.load(projectId, {
  resolve: { tasks: { $each: true } }
});

project // => null
```
</CodeGroup>
If any item in a list is inaccessible to the user, the entire load operation will fail and return `null`. This is because lists expect all their items to be accessible - a partially loaded list could lead to data inconsistencies.

#### Reading a non-resolved inaccessible reference

When trying to load an object with an inaccessible reference without directly resolving it:

<CodeGroup>
```ts twoslash

const TeamMember = co.map({
  name: z.string(),
});

const Task = co.map({
  title: z.string(),
  assignee: z.optional(TeamMember),
  get subtasks(): CoListSchema<typeof Task> { return co.list(Task) },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
  owner: TeamMember,
});

const projectId = "co_123";
// ---cut-before---
const project = await Project.load(projectId, {
  resolve: true
});

project // => Project

// The user doesn't have access to the owner
project?.owner // => always null
```
</CodeGroup>

The load operation will succeed and return the object, but the inaccessible reference will always be `null`.


#### Deep loading lists with shared items

When loading a list with shared items, you can use the `$onError` option to safely load the list skipping any inaccessible items.

This is especially useful when in your app access to these items might be revoked.

This way the inaccessible items are replaced with `null` in the returned list.

<CodeGroup>
```ts twoslash

const me = await createJazzTestAccount();
const account2 = await createJazzTestAccount();

const Person = co.map({
  name: z.string(),
});

const Friends = co.list(Person);

const privateGroup = Group.create({ owner: account2 });
const publicGroup = Group.create({ owner: me });

// ---cut-before---
const source = co.list(Person).create(
  [
    Person.create(
      {
        name: "Jane",
      },
      privateGroup, // We don't have access to Jane
    ),
    Person.create(
      {
        name: "Alice",
      },
      publicGroup, // We have access to Alice
    ),
  ],
  publicGroup,
);

const friends = await co.list(Person).load(source.id, {
  resolve: {
    $each: { $onError: null }
  },
  loadAs: me,
});

// Thanks to $onError catching the errors, the list is loaded
// because we have access to friends
console.log(friends); // Person[]

// Jane is null because we lack access rights
// and we have used $onError to catch the error on the list items
console.log(friends?.[0]); // null

// Alice is not null because we have access
// the type is nullable because we have used $onError
console.log(friends?.[1]); // Person
```
</CodeGroup>

The `$onError` works as a "catch" clause option to block any error in the resolved children.

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const account2 = await createJazzTestAccount();


const Dog = co.map({
  name: z.string(),
});

const Person = co.map({
  name: z.string(),
  dog: Dog,
});

const User = co.map({
  name: z.string(),
  friends: co.list(Person),
});

const privateGroup = Group.create({ owner: account2 });
const publicGroup = Group.create({ owner: me });

// ---cut-before---
const source = co.list(Person).create(
  [
    Person.create(
      {
        name: "Jane",
        dog: Dog.create(
          { name: "Rex" },
          privateGroup,
        ), // We don't have access to Rex
      },
      publicGroup,
    ),
  ],
  publicGroup,
);

const friends = await co.list(Person).load(source.id, {
  resolve: {
    $each: { dog: true, $onError: null }
  },
  loadAs: me,
});

// Jane is null because we don't have access to Rex
// and we have used $onError to catch the error on the list items
console.log(friends?.[0]); // null
```
</CodeGroup>

We can actually use `$onError` everywhere in the resolve query, so we can use it to catch the error on dog:

<CodeGroup>
```ts twoslash
const me = await createJazzTestAccount();
const account2 = await createJazzTestAccount();


const Dog = co.map({
  name: z.string(),
});

const Person = co.map({
  name: z.string(),
  dog: Dog,
});

const User = co.map({
  name: z.string(),
  friends: co.list(Person),
});

const privateGroup = Group.create({ owner: account2 });
const publicGroup = Group.create({ owner: me });

const source = co.list(Person).create(
  [
    Person.create(
      {
        name: "Jane",
        dog: Dog.create(
          { name: "Rex" },
          privateGroup,
        ), // We don't have access to Rex
      },
      publicGroup,
    ),
  ],
  publicGroup,
);

// ---cut-before---
const friends = await co.list(Person).load(source.id, {
  resolve: {
    $each: { dog: { $onError: null } }
  },
  loadAs: me,
});

// Jane now is not-nullable at type level because
// we have moved $onError down to the dog field
//
// This also means that if we don't have access to Jane
// the entire friends list will be null
console.log(friends?.[0]); // => Person

// Jane's dog is null because we don't have access to Rex
// and we have used $onError to catch the error
console.log(friends?.[0]?.dog); // => null
```
</CodeGroup>

## Type Safety with Loaded Type

Jazz provides the `Loaded` type to help you define and enforce the structure of deeply loaded data in your application. This makes it easier to ensure that components receive the data they expect with proper TypeScript validation.

The `Loaded` type is especially useful when passing data between components, as it guarantees that all necessary nested data has been loaded:

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash

const TeamMember = co.map({
  name: z.string(),
});

const Task = co.map({
  title: z.string(),
  assignee: z.optional(TeamMember),
  get subtasks(): CoListSchema<typeof Task> {
    return co.list(Task);
  },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
  owner: TeamMember,
});

// ---cut-before---
// Define a type that includes loaded nested data
type ProjectWithTasks = Loaded<
  typeof Project,
  {
    tasks: { $each: true };
  }
>;

// Component that expects a fully loaded project
function TaskList({ project }: { project: ProjectWithTasks }) {
  // TypeScript knows tasks are loaded, so this is type-safe
  return (
    <ul>
      {project.tasks.map((task) => (
        <li key={task.id}>{task.title}</li>
      ))}
    </ul>
  );
}

// For more complex resolutions
type FullyLoadedProject = Loaded<
  typeof Project,
  {
    tasks: {
      $each: {
        subtasks: true;
        assignee: true;
      };
    };
    owner: true;
  }
>;

// Function that requires deeply loaded data
function processProject(project: FullyLoadedProject) {
  // Safe access to all loaded properties
  console.log(`Project ${project.name} owned by ${project.owner.name}`);

  project.tasks.forEach((task) => {
    console.log(`Task: ${task.title}, Assigned to: ${task.assignee?.name}`);
    console.log(`Subtasks: ${task.subtasks.length}`);
  });
}

```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework="vanilla">
<CodeGroup>
```ts twoslash

const TeamMember = co.map({
  name: z.string(),
});

const Task = co.map({
  title: z.string(),
  assignee: z.optional(TeamMember),
  get subtasks(): CoListSchema<typeof Task> {
    return co.list(Task);
  },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
  owner: TeamMember,
});

// ---cut-before---
// Define a type that includes loaded nested data
type ProjectWithTasks = Loaded<
  typeof Project,
  {
    tasks: { $each: true };
  }
>;

// Function that expects loaded data
async function taskList({ project }: { project: ProjectWithTasks }) {
  // TypeScript knows tasks are loaded, so this is type-safe
  return project.tasks.map((task) => task.title).join(`\n - `);
}

// For more complex resolutions
type FullyLoadedProject = Loaded<
  typeof Project,
  {
    tasks: {
      $each: {
        title: true;
        subtasks: true;
        assignee: true;
      };
    };
    owner: true;
  }
>;

// Function that requires deeply loaded data
function processProject(project: FullyLoadedProject) {
  // Safe access to all loaded properties
  console.log(`Project ${project.name} owned by ${project.owner.name}`);

  project.tasks.forEach((task) => {
    console.log(`Task: ${task.title}, Assigned to: ${task.assignee?.name}`);
    console.log(`Subtasks: ${task.subtasks.length}`);
  });
}
```
</CodeGroup>
</ContentByFramework>

Using the `Loaded` type helps catch errors at compile time rather than runtime, ensuring that your components and functions receive data with the proper resolution depth. This is especially useful for larger applications where data is passed between many components.

## Ensuring Data is Loaded

Sometimes you need to make sure data is loaded before proceeding with an operation. The `ensureLoaded` method lets you guarantee that a CoValue and its referenced data are loaded to a specific depth:

<CodeGroup>
```ts twoslash

const TeamMember = co.map({
  name: z.string(),
});

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "completed"]),
  assignee: z.string().optional(),
  get subtasks(): CoListSchema<typeof Task> {
    return co.list(Task);
  },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
  owner: TeamMember,
});

// ---cut-before---
async function completeAllTasks(projectId: string) {
  // Ensure the project is loaded
  const project = await Project.load(projectId, { resolve: true });
  if (!project) return;

  // Ensure tasks are loaded
  const loadedProject = await project.ensureLoaded({
    resolve: {
      tasks: {
        $each: true,
      },
    },
  });

  // Now we can safely access and modify tasks
  loadedProject.tasks.forEach((task) => {
    task.status = "completed";
  });
}
```
</CodeGroup>



## Best Practices

1. **Be explicit about resolution depths**: Always specify exactly what you need
2. **Use framework integrations**: They handle subscription lifecycle automatically
3. **Clean up subscriptions**: Always store and call the unsubscribe function when you're done
4. **Handle all loading states**: Check for undefined (loading), null (not found), and success states
5. **Use the Loaded type**: Add compile-time type safety for components that require specific resolution patterns



### Groups, permissions & sharing

#### Groups as permission scopes

# Groups as permission scopes

Every CoValue has an owner, which can be a `Group` or an `Account`.

You can use a `Group` to grant access to a CoValue to **multiple users**. These users can
have different roles, such as "writer", "reader" or "admin".

## Creating a Group

Here's how you can create a `Group`.

<CodeGroup>
```tsx twoslash

const group = Group.create();
```
</CodeGroup>

The `Group` itself is a CoValue, and whoever owns it is the initial admin.

You typically add members using [public sharing](/docs/groups/sharing#public-sharing) or [invites](/docs/groups/sharing#invites).
But if you already know their ID, you can add them directly (see below).

## Adding group members by ID

You can add group members by ID by using `Account.load` and `Group.addMember`.

<CodeGroup>
```tsx twoslash

const bobsID = "co_z123" as ID<Account>;

// ---cut---

const group = Group.create();

const bob = await Account.load(bobsID);

if (bob) {
  group.addMember(bob, "writer");
}
```
</CodeGroup>

**Note:** if the account ID is of type `string`, because it comes from a URL parameter or something similar, you need to cast it to `ID<Account>` first:

<CodeGroup>
```tsx twoslash
const bobsID = "co_z123" as ID<Account>;

const group = Group.create();

// ---cut---

const bob = await Account.load(bobsID as ID<Account>);

if (bob) {
  group.addMember(bob, "writer");
}
```
</CodeGroup>

## Changing a member's role

To change a member's role, use the `addMember` method.

<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();
const group = Group.create();
// ---cut---
group.addMember(bob, "reader");
```
</CodeGroup>

Bob just went from a writer to a reader.

**Note:** only admins can change a member's role.

## Removing a member

To remove a member, use the `removeMember` method.

<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();
const group = Group.create();
// ---cut---
group.removeMember(bob);
```
</CodeGroup>

Rules:
- All roles can remove themselves.
- Only admins can remove other users.
- An admin cannot remove other admins.
- As an admin, you cannot remove yourself if you are the only admin in the Group, because there has to be at least one admin present.

## Getting the Group of an existing CoValue

You can get the group of an existing CoValue by using `coValue._owner`.

<CodeGroup>
```ts twoslash
const existingCoValue = await createJazzTestAccount();

const MyCoMap = co.map({
  color: z.string(),
});

// ---cut---
const group = existingCoValue._owner;
const newValue = MyCoMap.create(
  { color: "red"},
  { owner: group }
);
```
</CodeGroup>

Because `._owner` can be an `Account` or a `Group`, in cases where you specifically need to use `Group` methods (such as for adding members or getting your own role), you can cast it to assert it to be a Group:

<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();

const MyCoMap = co.map({
  color: z.string(),
});

const existingCoValue = MyCoMap.create(
  { color: "red"},
  { owner: bob }
);

// ---cut---

const group = existingCoValue._owner.castAs(Group);
group.addMember(bob, "writer");

const role = group.getRoleOf(bob.id);
```
</CodeGroup>

## Checking the permissions

You can check the permissions of an account on a CoValue by using the `canRead`, `canWrite` and `canAdmin` methods.

<CodeGroup>
```ts twoslash

const MyCoMap = co.map({
  color: z.string(),
});
// ---cut---
const value = await MyCoMap.create({ color: "red"})
const me = Account.getMe();

if (me.canAdmin(value)) {
  console.log("I can share value with others"); 
} else if (me.canWrite(value)) {
  console.log("I can edit value");
} else if (me.canRead(value)) {
  console.log("I can view value");
} else {
  console.log("I cannot access value");
}
```
</CodeGroup>

To check the permissions of another account, you need to load it first:

<CodeGroup>
```ts twoslash

const MyCoMap = co.map({
  color: z.string(),
});
const account = await createJazzTestAccount();
const accountID = account.id;
// ---cut---
const value = await MyCoMap.create({ color: "red"})
const bob = await Account.load(accountID);

if (bob) {
  if (bob.canAdmin(value)) {
    console.log("Bob can share value with others");
  } else if (bob.canWrite(value)) {
    console.log("Bob can edit value");
  } else if (bob.canRead(value)) {
    console.log("Bob can view value");
  } else {
    console.log("Bob cannot access value");
  }
}
```
</CodeGroup>



#### Public sharing & invites

# Public sharing and invites

## Public sharing

You can share CoValues publicly by setting the `owner` to a `Group`, and granting
access to "everyone".

<CodeGroup>
```ts twoslash
// ---cut---
const group = Group.create();
group.addMember("everyone", "writer");
```
</CodeGroup>

This is done in the [chat example](https://github.com/garden-co/jazz/tree/main/examples/chat) where anyone can join the chat, and send messages.

You can also [add members by Account ID](/docs/groups/intro#adding-group-members-by-id).

## Invites

You can grant users access to a CoValue by sending them an invite link.

This is used in the [pet example](https://github.com/garden-co/jazz/tree/main/examples/pets)
and the [todo example](https://github.com/garden-co/jazz/tree/main/examples/todo).

<ContentByFramework framework={["react", "react-native", "react-native-expo", "vue", "svelte"]}>
<CodeGroup>
```ts twoslash

const Organization = co.map({
  name: z.string(),
});
const organization = Organization.create({ name: "Garden Computing" });
// ---cut---

createInviteLink(organization, "writer"); // or reader, admin, writeOnly
```
</CodeGroup>
</ContentByFramework>

It generates a URL that looks like `.../invite/[CoValue ID]/[inviteSecret]`

In your app, you need to handle this route, and let the user accept the invitation,
as done [here](https://github.com/garden-co/jazz/tree/main/examples/pets/src/2_main.tsx).

<CodeGroup>
```ts twoslash

const Organization = co.map({
  name: z.string(),
});
const organization = Organization.create({ name: "Garden Computing" });
const organizationID = organization.id;
// ---cut---

useAcceptInvite({
  invitedObjectSchema: Organization,
  onAccept: (organizationID) => {
    console.log("Accepted invite!")
    // navigate to the organization page
  },
});
```
</CodeGroup>


### Requesting Invites

To allow a non-group member to request an invitation to a group you can use the `writeOnly` role.
This means that users only have write access to a specific requests list (they can't read other requests). 
However, Administrators can review and approve these requests.

Create the data models.

<CodeGroup>
```ts twoslash
// ---cut---
const JoinRequest = co.map({
  account: Account,
  status: z.literal(["pending", "approved", "rejected"]),
});

const RequestsList = co.list(JoinRequest);
```
</CodeGroup>

Set up the request system with appropriate access controls.

<CodeGroup>
```ts twoslash

const JoinRequest = co.map({
  account: Account,
  status: z.literal(["pending", "approved", "rejected"]),
});

const RequestsList = co.list(JoinRequest);

// ---cut-before---
function createRequestsToJoin() {
  const requestsGroup = Group.create();
  requestsGroup.addMember("everyone", "writeOnly");

  return RequestsList.create([], requestsGroup);
}

async function sendJoinRequest(
  requestsList: Loaded<typeof RequestsList>,
  account: Account,
) {
  const request = JoinRequest.create(
    {
      account,
      status: "pending",
    },
    requestsList._owner // Inherit the access controls of the requestsList
  );

  requestsList.push(request);

  return request;
}
```
</CodeGroup>

Using the write-only access users can submit requests that only administrators can review and approve.

<CodeGroup>
```ts twoslash

const JoinRequest = co.map({
  account: Account,
  status: z.literal(["pending", "approved", "rejected"]),
});

const RequestsList = co.list(JoinRequest);

const RequestsToJoin = co.map({
  writeOnlyInvite: z.string(),
  requests: RequestsList,
});

// ---cut-before---
async function approveJoinRequest(
  joinRequest: Loaded<typeof JoinRequest, { account: true }>,
  targetGroup: Group,
) {
  const account = await Account.load(joinRequest._refs.account.id);

  if (account) {
    targetGroup.addMember(account, "reader");
    joinRequest.status = "approved";

    return true;
  } else {
    return false;
  }
}
```
</CodeGroup>



#### Group inheritance

# Group Inheritance

Groups can inherit members from other groups using the `extend` method. 

When a group extends another group, members of the parent group will become automatically part of the child group.

## Basic Usage

Here's how to extend a group:

<CodeGroup>
```ts twoslash
// ---cut---
const playlistGroup = Group.create();
const trackGroup = Group.create();

// This way track becomes visible to the members of playlist
trackGroup.extend(playlistGroup);
```
</CodeGroup>

When you extend a group:
- Members of the parent group get access to the child group
- Their roles are inherited (with some exceptions, see [below](#role-inheritance-rules))
- Removing a member from the parent group also removes their access to child groups

## Inheriting members but overriding their role

In some cases you might want to inherit all members from a parent group but override/flatten their roles to the same specific role in the child group. You can do so by passing an "override role" as a second argument to `extend`:

<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();
// ---cut---
const organizationGroup = Group.create();
organizationGroup.addMember(bob, "admin");

const billingGroup = Group.create();

// This way the members of the organization can only read the billing data
billingGroup.extend(organizationGroup, "reader"); 
```
</CodeGroup>

The "override role" works in both directions:

<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();
const alice = await createJazzTestAccount();
// ---cut---
const parentGroup = Group.create();
parentGroup.addMember(bob, "reader");
parentGroup.addMember(alice, "admin");

const childGroup = Group.create();
childGroup.extend(parentGroup, "writer");

// Bob and Alice are now writers in the child group
```
</CodeGroup>

## Multiple Levels of Inheritance

Groups can be extended multiple levels deep:

<CodeGroup>
```ts twoslash
// ---cut---
const grandParentGroup = Group.create();
const parentGroup = Group.create();
const childGroup = Group.create(); 

childGroup.extend(parentGroup);
parentGroup.extend(grandParentGroup);
```
</CodeGroup>

Members of the grandparent group will get access to all descendant groups based on their roles.

## Permission Changes

When you remove a member from a parent group, they automatically lose access to all child groups. We handle key rotation automatically to ensure security.

<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();
const parentGroup = Group.create();
// ---cut---
// Remove member from parent
await parentGroup.removeMember(bob);

// Bob loses access to both parent and child groups
```
</CodeGroup>

## Role Inheritance Rules

If the account is already a member of the child group, it will get the more permissive role:
<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();
// ---cut---
const parentGroup = Group.create();
parentGroup.addMember(bob, "reader");

const childGroup = Group.create();
parentGroup.addMember(bob, "writer");
childGroup.extend(parentGroup);

// Bob stays a writer because his role is higher
// than the inherited reader role.
```
</CodeGroup>

When extending groups, only admin, writer and reader roles are inherited:
<CodeGroup>
```ts twoslash
const bob = await createJazzTestAccount();
// ---cut---
const parentGroup = Group.create();
parentGroup.addMember(bob, "writeOnly");

const childGroup = Group.create();
childGroup.extend(parentGroup);

// Bob does not become a member of the child group
```
</CodeGroup>

To extend a group:

1. The current account must be an admin in the child group
2. The current account must be a member of the parent group

<CodeGroup>
```ts twoslash
const Company = co.map({
  name: z.string(),
});
const company = Company.create({ name: "Garden Computing" });
// ---cut---
const companyGroup = company._owner.castAs(Group)
const teamGroup = Group.create();

// Works only if I'm a member of companyGroup
teamGroup.extend(companyGroup); 
```
</CodeGroup>

## Revoking a group extension

You can revoke a group extension by using the `revokeExtend` method:

<CodeGroup>
```ts twoslash
// ---cut---
const parentGroup = Group.create();
const childGroup = Group.create();

childGroup.extend(parentGroup); 

// Revoke the extension
await childGroup.revokeExtend(parentGroup);
```
</CodeGroup>

## Getting all parent groups

You can get all the parent groups of a group by calling the `getParentGroups` method:

<CodeGroup>
```ts twoslash
// ---cut---
const childGroup = Group.create();
const parentGroup = Group.create();
childGroup.extend(parentGroup);

console.log(childGroup.getParentGroups()); // [parentGroup]
```
</CodeGroup>

## Example: Team Hierarchy

Here's a practical example of using group inheritance for team permissions:

<CodeGroup>
```ts twoslash
const CEO = await createJazzTestAccount();
const teamLead = await createJazzTestAccount();
const developer = await createJazzTestAccount();
const client = await createJazzTestAccount();
// ---cut---
// Company-wide group
const companyGroup = Group.create();
companyGroup.addMember(CEO, "admin");

// Team group with elevated permissions
const teamGroup = Group.create();
teamGroup.extend(companyGroup); // Inherits company-wide access
teamGroup.addMember(teamLead, "admin");
teamGroup.addMember(developer, "writer");

// Project group with specific permissions
const projectGroup = Group.create();
projectGroup.extend(teamGroup); // Inherits team permissions
projectGroup.addMember(client, "reader"); // Client can only read project items
```
</CodeGroup>

This creates a hierarchy where:
- The CEO has admin access to everything
- Team members get writer access to team and project content
- Team leads get admin access to team and project content
- The client can only read project content



### Authentication

#### Overview

# Authentication in Jazz

Jazz authentication is based on cryptographic keys ("Account keys"). Their public part represents a user's identity, their secret part lets you act as that user.

## Authentication Flow

When a user first opens your app, they'll be in one of these states:

- **Anonymous Authentication**: Default starting point where Jazz automatically creates a local account on first visit. Data persists on one device and can be upgraded to a full account.

- **Authenticated Account**: Full account accessible across multiple devices using [passkeys](./passkey), [passphrases](./passphrase), or third-party authentications, such as [Clerk](./clerk).

- **Guest Mode**: No account, read-only access to public content. Users can browse but can't save data or sync.

Learn more about these states in the [Authentication States](./authentication-states) documentation.

Without authentication, users are limited to using the application on only one device.

When a user logs out of an Authenticated Account, they return to the Anonymous Authentication state with a new local account.

Here's what happens during registration and login:

- **Register**: When a user registers with an authentication provider, their Anonymous account credentials are stored in the auth provider, and the account is marked as Authenticated. The user keeps all their existing data.

- **Login**: When a user logs in with an authentication provider, their Anonymous account is discarded and the credentials are loaded from the auth provider. Data from the Anonymous account can be transferred using the [onAnonymousAccountDiscarded handler](./authentication-states#migrating-data-from-anonymous-to-authenticated-account).

## Available Authentication Methods

Jazz provides several ways to authenticate users:

- [**Passkeys**](./passkey): Secure, biometric authentication using WebAuthn
- [**Passphrases**](./passphrase): Bitcoin-style word phrases that users store
- [**Clerk Integration**](./clerk): Third-party authentication service with OAuth support
- [**Jazz Cloud Integration**](./jazz-cloud): Authentication service provided through Jazz Cloud
- [**Self-Hosting**](./self-hosting): Self-hosted authentication service



#### Authentication States

# Authentication States

Jazz provides three distinct authentication states that determine how users interact with your app: **Anonymous Authentication**, **Guest Mode**, and **Authenticated Account**.

## Anonymous Authentication

When a user loads a Jazz application for the first time, we create a new Account by generating keys and storing them locally:

- Users have full accounts with unique IDs
- Data persists between sessions on the same device
- Can be upgraded to a full account (passkey, passphrase, etc.)
- Data syncs across the network (if enabled)

## Authenticated Account

**Authenticated Account** provides full multi-device functionality:

- Persistent identity across multiple devices
- Full access to all application features
- Data can sync across all user devices
- Multiple authentication methods available

## Guest Mode

**Guest Mode** provides a completely accountless context:

- No persistent identity or account
- Only provides access to publicly readable content
- Cannot save or sync user-specific data
- Suitable for read-only access to public resources

## Detecting Authentication State

You can detect the current authentication state using `useAccountOrGuest` and `useIsAuthenticated`.

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash
// ---cut---

function AuthStateIndicator() {
  const { me } = useAccountOrGuest();
  const isAuthenticated = useIsAuthenticated();

  // Check if guest mode is enabled in JazzProvider
  const isGuest = me._type !== "Account"

  // Anonymous authentication: has an account but not fully authenticated
  const isAnonymous =  me._type === "Account" && !isAuthenticated;
  return (
    <div>
      {isGuest && <span>Guest Mode</span>}
      {isAnonymous && <span>Anonymous Account</span>}
      {isAuthenticated && <span>Authenticated</span>}
    </div>
  );
}
```
</CodeGroup>
</ContentByFramework>

## Migrating data from anonymous to authenticated account

When a user signs up, their anonymous account is transparently upgraded to an authenticated account, preserving all their data.

However, if a user has been using your app anonymously and later logs in with an existing account, their anonymous account data would normally be discarded. To prevent data loss, you can use the `onAnonymousAccountDiscarded` handler.

This example from our [music player example app](https://github.com/garden-co/jazz/tree/main/examples/music-player) shows how to migrate data:

<CodeGroup>
```ts twoslash

class MusicTrack extends CoMap {
  title = coField.string;
  duration = coField.number;
  isExampleTrack = coField.optional.boolean;
}
class ListOfTracks extends CoList.Of(coField.ref(MusicTrack)) {}
class Playlist extends CoMap {
  title = coField.string;
  tracks = coField.ref(ListOfTracks);
}
class MusicaAccountRoot extends CoMap {
  rootPlaylist = coField.ref(Playlist);
}
class MusicaAccount extends Account {
  root = coField.ref(MusicaAccountRoot);
}

// ---cut---
export async function onAnonymousAccountDiscarded(
  anonymousAccount: MusicaAccount,
) {
  const { root: anonymousAccountRoot } = await anonymousAccount.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: {
            $each: true,
          },
        },
      },
    },
  });

  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
      },
    },
  });

  for (const track of anonymousAccountRoot.rootPlaylist.tracks) {
    if (track.isExampleTrack) continue;

    const trackGroup = track._owner.castAs(Group);
    trackGroup.addMember(me, "admin");

    me.root.rootPlaylist.tracks.push(track);
  }
}
```
</CodeGroup>

To see how this works, try uploading a song in the [music player demo](https://music-demo.jazz.tools/) and then log in with an existing account.

## Provider Configuration for Authentication

You can configure how authentication states work in your app with the [JazzProvider](/docs/project-setup/providers/). The provider offers several options that impact authentication behavior:

- `guestMode`: Enable/disable Guest Mode
- `onAnonymousAccountDiscarded`: Handle data migration when switching accounts
- `sync.when`: Control when data synchronization happens
- `defaultProfileName`: Set default name for new user profiles

For detailed information on all provider options, see [Provider Configuration options](/docs/project-setup/providers/#additional-options).

## Controlling sync for different authentication states

You can control network sync with [Providers](/docs/project-setup/providers/) based on authentication state:

- `when: "always"`: Sync is enabled for both Anonymous Authentication and Authenticated Account
- `when: "signedUp"`: Sync is enabled when the user is authenticated
- `when: "never"`: Sync is disabled, content stays local

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash
const apiKey = "you@example.com";
function App() {
  return <div>Hello World</div>;
}
// ---cut---
<JazzProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
     // Controls when sync is enabled for
     // both Anonymous Authentication and Authenticated Account
    when: "always", // or "signedUp" or "never"
  }}
>
  <App />
</JazzProvider>
```
</CodeGroup>
</ContentByFramework>

### Disable sync for Anonymous Authentication

You can disable network sync to make your app local-only under specific circumstances.

For example, you may want to give users with Anonymous Authentication the opportunity to try your app locally-only (incurring no sync traffic), then enable network sync only when the user is fully authenticated.

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash
const apiKey = "you@example.com";
function App() {
  return <div>Hello World</div>;
}
// ---cut---
<JazzProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
     // This makes the app work in local mode when using Anonymous Authentication
    when: "signedUp",
  }}
>
  <App />
</JazzProvider>
```
</CodeGroup>
</ContentByFramework>

### Configuring Guest Mode Access

You can configure Guest Mode access with the `guestMode` prop for [Providers](/docs/project-setup/providers/).

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash
const apiKey = "you@example.com";
function App() {
  return <div>Hello World</div>;
}
// ---cut---
<JazzProvider
  // Enable Guest Mode for public content
  guestMode={true}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // Only sync for authenticated users
    when: "signedUp",
  }}
>
  <App />
</JazzProvider>
```
</CodeGroup>
</ContentByFramework>

For more complex behaviours, you can manually control sync by statefully switching when between `"always"` and `"never"`.



#### Passkey

# Passkey Authentication

Passkey authentication is fully local-first and the most secure of the auth methods that Jazz provides because keys are managed by the device/operating system itself.

## How it works

Passkey authentication is based on the [Web Authentication API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Authentication_API) and uses familiar FaceID/TouchID flows that users already know how to use.

## Key benefits

- **Most secure**: Keys are managed by the device/OS
- **User-friendly**: Uses familiar biometric verification (FaceID/TouchID)
- **Cross-device**: Works across devices with the same biometric authentication
- **No password management**: Users don't need to remember or store anything
- **Wide support**: Available in most modern browsers

## Implementation

<ContentByFramework framework="react">
Using passkeys in Jazz is as easy as this:

<CodeGroup>
```tsx twoslash
type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}
// ---cut---
export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [username, setUsername] = useState("");

  const auth = usePasskeyAuth({ // Must be inside the JazzProvider!
    appName: "My super-cool web app",
  });

  if (auth.state === "signedIn") { // You can also use `useIsAuthenticated()`
    return <div>You are already signed in</div>;
  }

  const handleSignUp = async () => {
    await auth.signUp(username);
    onOpenChange(false);
  };

  const handleLogIn = async () => {
    await auth.logIn();
    onOpenChange(false);
  };

  return (
    <div>
      <button onClick={handleLogIn}>Log in</button>
      <input type="text" value={username} onChange={(e) => setUsername(e.target.value)} />
      <button onClick={handleSignUp}>Sign up</button>
    </div>
  );
}
```
</CodeGroup>
</ContentByFramework>

## Examples

You can try passkey authentication using our [passkey example](https://passkey-demo.jazz.tools/) or the [music player demo](https://music-demo.jazz.tools/).

## When to use Passkeys

Passkeys are ideal when:
- Security is a top priority
- You want the most user-friendly authentication experience
- You're targeting modern browsers and devices
- You want to eliminate the risk of password-based attacks

## Limitations and considerations

- Requires hardware/OS support for biometric authentication
- Not supported in older browsers (see browser support below)
- Requires a fallback method for unsupported environments

### Browser Support

[Passkeys are supported in most modern browsers](https://caniuse.com/passkeys).

For older browsers, we recommend using [passphrase authentication](./passphrase) as a fallback.

## Additional resources

For more information about the Web Authentication API and passkeys:
- [WebAuthn.io](https://webauthn.io/)
- [MDN Web Authentication API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Authentication_API)



#### Passphrase

# Passphrase Authentication

Passphrase authentication lets users log into any device using a recovery phrase consisting of multiple words (similar to cryptocurrency wallets). Users are responsible for storing this passphrase safely.

## How it works

When a user creates an account with passphrase authentication:

1. Jazz generates a unique recovery phrase derived from the user's cryptographic keys
2. This phrase consists of words from a wordlist
3. Users save this phrase and enter it when logging in on new devices

You can use one of the ready-to-use wordlists from the [BIP39 repository](https://github.com/bitcoinjs/bip39/tree/a7ecbfe2e60d0214ce17163d610cad9f7b23140c/src/wordlists) or create your own.

## Key benefits

- **Portable**: Works across any device, even without browser or OS support
- **User-controlled**: User manages their authentication phrase
- **Flexible**: Works with any wordlist you choose
- **Offline capable**: No external dependencies

## Implementation

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash
// @filename: wordlist.ts
export const wordlist = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon", "mango", "orange", "pear", "quince", "raspberry", "strawberry", "tangerine", "uva", "watermelon", "xigua", "yuzu", "zucchini"];
// @filename: AuthModal.tsx
type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}
// ---cut---

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [loginPassphrase, setLoginPassphrase] = useState("");

  const auth = usePassphraseAuth({ // Must be inside the JazzProvider!
    wordlist: wordlist,
  });

  if (auth.state === "signedIn") { // You can also use `useIsAuthenticated()`
    return <div>You are already signed in</div>;
  }

  const handleSignUp = async () => {
    await auth.signUp();
    onOpenChange(false);
  };

  const handleLogIn = async () => {
    await auth.logIn(loginPassphrase);
    onOpenChange(false);
  };

  return (
    <div>
      <label>
        Your current passphrase
        <textarea
          readOnly
          value={auth.passphrase}
          rows={5}
        />
      </label>
      <button onClick={handleSignUp}>I have stored my passphrase</button>
      <label>
        Log in with your passphrase
        <textarea
          value={loginPassphrase}
          onChange={(e) => setLoginPassphrase(e.target.value)}
          placeholder="Enter your passphrase"
          rows={5}
          required
        />
      </label>
      <button onClick={handleLogIn}>Log in</button>
    </div>
  );
}
```
</CodeGroup>
</ContentByFramework>

<ContentByFramework framework={["react-native", "react-native-expo"]}>
<CodeGroup>
```tsx twoslash
// @filename: wordlist.ts
export const wordlist = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon", "mango", "orange", "pear", "quince", "raspberry", "strawberry", "tangerine", "uva", "watermelon", "xigua", "yuzu", "zucchini"];
// @filename: AuthModal.tsx

type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}
// ---cut---

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [loginPassphrase, setLoginPassphrase] = useState("");

  const auth = usePassphraseAuth({
    wordlist: wordlist,
  });

  if (auth.state === "signedIn") {
    return <Text>You are already signed in</Text>;
  }

  const handleSignUp = async () => {
    await auth.signUp();
    onOpenChange(false);
  };

  const handleLogIn = async () => {
    await auth.logIn(loginPassphrase);
    onOpenChange(false);
  };

  return (
    <View>
      <View>
        <Text>Your current passphrase</Text>
        <TextInput
          editable={false}
          value={auth.passphrase}
          multiline
          numberOfLines={5}
        />
      </View>

      <Button
        title="I have stored my passphrase"
        onPress={handleSignUp}
      />

      <View>
        <Text>Log in with your passphrase</Text>
        <TextInput
          value={loginPassphrase}
          onChangeText={setLoginPassphrase}
          placeholder="Enter your passphrase"
          multiline
          numberOfLines={5}
        />
      </View>

      <Button
        title="Log in"
        onPress={handleLogIn}
      />
    </View>
  );
}
```
</CodeGroup>
</ContentByFramework>

## Examples

You can see passphrase authentication in our [passphrase example](https://passphrase-demo.jazz.tools/) or the [todo list demo](https://todo-demo.jazz.tools/).

## When to use Passphrases

Passphrase authentication is ideal when:

- You need to support older browsers without WebAuthn capabilities
- Your users need to access the app on many different devices
- You want a fallback authentication method alongside passkeys

## Limitations and considerations

- **User responsibility**: Users must securely store their passphrase
- **Recovery concerns**: If a user loses their passphrase, they cannot recover their account
- **Security risk**: Anyone with the passphrase can access the account
- **User experience**: Requires users to enter a potentially long phrase

Make sure to emphasize to your users:
1. Store the passphrase in a secure location (password manager, written down in a safe place)
2. The passphrase is the only way to recover their account
3. Anyone with the passphrase can access the account



#### Clerk

### react-native Implementation

# Clerk Authentication

We do not currently support Clerk in React Native, but we do have support for [React Native Expo](/docs/react-native-expo/authentication/clerk).



### Design patterns

#### Autosaving forms

# How to write autosaving forms to create and update CoValues

This guide shows you a simple and powerful way to implement forms for creating and updating CoValues.

We'll build:
1. An update form that saves changes as you make them, removing the need for a save button.
2. A create form that autosaves your changes into a draft, so you can come back to it later.

[See the full example here.](https://github.com/garden-co/jazz/tree/main/examples/form)

**Note**: If you do need a save button on your update form, this guide is not for you. Another option
is to use [react-hook-form](https://www.react-hook-form.com), which you can see in [this example](https://github.com/garden-co/jazz/tree/main/examples/password-manager).

## Updating a CoValue

To update a CoValue, we simply assign the new value directly as changes happen. These changes are synced to the server.

<CodeGroup>
```tsx
<input
  type="text"
  value={order.name}
  onChange={(e) => order.name = e.target.value}
/>
```
</CodeGroup>

It's that simple!

## Creating a CoValue

However, when creating a CoValue, the CoValue does not exist yet, so we don't have the advantages previously mentioned.

There's a way around this, and it provides unexpected benefits too.

### Using a Draft CoValue

Let's say we have a CoValue called `BubbleTeaOrder`. We can create a "draft" CoValue,
which is an empty version of a `BubbleTeaOrder`, that we can then modify when we are "creating"
a new CoValue.

A `DraftBubbleTeaOrder` is essentially a copy of `BubbleTeaOrder`, but with all the fields made optional.

<CodeGroup>
```tsx twoslash
// ---cut---
// schema.ts
export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
});
```
</CodeGroup>

## Writing the components in React

Let's write the form component that will be used for both create and update.

<CodeGroup>
```tsx twoslash

export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
});
// ---cut---
// OrderForm.tsx
export function OrderForm({
  order,
  onSave,
}: {
  order: Loaded<typeof BubbleTeaOrder> | Loaded<typeof DraftBubbleTeaOrder>;
  onSave?: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <form onSubmit={onSave}>
      <label>
        Name
        <input
          type="text"
          value={order.name}
          onChange={(e) => (order.name = e.target.value)}
          required
        />
      </label>

      {onSave && <button type="submit">Submit</button>}
    </form>
  );
}
```
</CodeGroup>

### Writing the edit form

To make the edit form, simply pass the `BubbleTeaOrder`.

<CodeGroup>
```tsx twoslash

export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
});

export function OrderForm({
  order,
  onSave,
}: {
  order: Loaded<typeof BubbleTeaOrder> | Loaded<typeof DraftBubbleTeaOrder>;
  onSave?: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <form onSubmit={onSave}>
      <label>
        Name
        <input
          type="text"
          value={order.name}
          onChange={(e) => (order.name = e.target.value)}
          required
        />
      </label>

      {onSave && <button type="submit">Submit</button>}
    </form>
  );
}
// ---cut---
// EditOrder.tsx
export function EditOrder(props: { id: string }) {
  const order = useCoState(BubbleTeaOrder, props.id);

  if (!order) return;

  return <OrderForm order={order} />;
}
```
</CodeGroup>

### Writing the create form

For the create form, we need to:
1. Create a draft order.
2. Edit the draft order.
3. Convert the draft order to a "real" order on submit.

Here's how that looks like:

<CodeGroup>
```tsx twoslash

export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
});

export const AccountRoot = co.map({
  draft: DraftBubbleTeaOrder,
});

export const JazzAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});

export function OrderForm({
  order,
  onSave,
}: {
  order: Loaded<typeof BubbleTeaOrder> | Loaded<typeof DraftBubbleTeaOrder>;
  onSave?: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <form onSubmit={onSave}>
      <label>
        Name
        <input
          type="text"
          value={order.name}
          onChange={(e) => (order.name = e.target.value)}
          required
        />
      </label>

      {onSave && <button type="submit">Submit</button>}
    </form>
  );
}
// ---cut---
// CreateOrder.tsx
export function CreateOrder() {
  const { me } = useAccount();
  const [draft, setDraft] = useState<Loaded<typeof DraftBubbleTeaOrder>>();

  useEffect(() => {
    setDraft(DraftBubbleTeaOrder.create({}));
  }, [me?.id]);

  const onSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!draft || !draft.name) return;

    const order = draft as Loaded<typeof BubbleTeaOrder>; // TODO: this should narrow correctly

    console.log("Order created:", order);
  };

  if (!draft) return;

  return <OrderForm order={draft} onSave={onSave} />;
}
```
</CodeGroup>

## Validation

In a `BubbleTeaOrder`, the `name` field is required, so it would be a good idea to validate this before turning the draft into a real order.

Update the schema to include a `validate` helper.

<CodeGroup>
```ts twoslash
// ---cut---
// schema.ts
export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
}).withHelpers((Self) => ({ // [!code ++:11]
  validate(draft: Loaded<typeof Self>) {
    const errors: string[] = [];

    if (!draft.name) {
      errors.push("Please enter a name.");
    }

    return { errors };
  },
}));
```
</CodeGroup>

Then perform the validation on submit.

<CodeGroup>
```tsx twoslash

export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
}).withHelpers((Self) => ({
  validate(draft: Loaded<typeof Self>) {
    const errors: string[] = [];

    if (!draft.name) {
      errors.push("Please enter a name.");
    }

    return { errors };
  },
}));

export const AccountRoot = co.map({
  draft: DraftBubbleTeaOrder,
});

export const JazzAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
});

export function OrderForm({
  order,
  onSave,
}: {
  order: Loaded<typeof BubbleTeaOrder> | Loaded<typeof DraftBubbleTeaOrder>;
  onSave?: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <form onSubmit={onSave}>
      <label>
        Name
        <input
          type="text"
          value={order.name}
          onChange={(e) => (order.name = e.target.value)}
          required
        />
      </label>

      {onSave && <button type="submit">Submit</button>}
    </form>
  );
}
// ---cut---
// CreateOrder.tsx
export function CreateOrder() {
  const { me } = useAccount();
  const [draft, setDraft] = useState<Loaded<typeof DraftBubbleTeaOrder>>();

  useEffect(() => {
    setDraft(DraftBubbleTeaOrder.create({}));
  }, [me?.id]);

  const onSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!draft) return;

    const validation = DraftBubbleTeaOrder.validate(draft); // [!code ++:5]
    if (validation.errors.length > 0) {
      console.log(validation.errors);
      return;
    }

    const order = draft as Loaded<typeof BubbleTeaOrder>;

    console.log("Order created:", order);
  };

  if (!draft) return;

  return <OrderForm order={draft} onSave={onSave} />;
}
```
</CodeGroup>

## Saving the user's work-in-progress

It turns out that using this pattern also provides a UX improvement.

By storing the draft in the user's account, they can come back to it anytime without losing their work. 🙌

<CodeGroup>
```ts twoslash
// ---cut---
// schema.ts
export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
});

export const AccountRoot = co.map({ // [!code ++:15]
  draft: DraftBubbleTeaOrder,
});

export const JazzAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
}).withMigration((account, creationProps?: { name: string }) => {
  if (account.root === undefined) {
    const draft = DraftBubbleTeaOrder.create({});

    account.root = AccountRoot.create({ draft });
  }
});
```
</CodeGroup>

Let's not forget to update the `AccountSchema`.

<CodeGroup>
```ts twoslash
// @filename: schema.tsx
export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
});

export const AccountRoot = co.map({
  draft: DraftBubbleTeaOrder,
});

export const JazzAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
}).withMigration((account, creationProps?: { name: string }) => {
  if (account.root === undefined) {
    const draft = DraftBubbleTeaOrder.create({});

    account.root = AccountRoot.create({ draft });
  }
});

// @filename: App.tsx
// ---cut---

export function MyJazzProvider({ children }: { children: React.ReactNode }) {
    return (
        <JazzProvider
            sync={{ peer: "wss://cloud.jazz.tools/?key=you@example.com" }}
            AccountSchema={JazzAccount} // [!code ++]
        >
            {children}
        </JazzProvider>
    );
}
```
</CodeGroup>

Instead of creating a new draft every time we use the create form, let's use the draft from the account root.

<CodeGroup>
```tsx twoslash
// @filename: schema.ts

export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
}).withHelpers((Self) => ({
  validate(draft: Loaded<typeof Self>) {
    const errors: string[] = [];

    if (!draft.name) {
      errors.push("Please enter a name.");
    }

    return { errors };
  },
}));

export const AccountRoot = co.map({
  draft: DraftBubbleTeaOrder,
});

export const JazzAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
}).withMigration((account, creationProps?: { name: string }) => {
  if (account.root === undefined) {
    const draft = DraftBubbleTeaOrder.create({});

    account.root = AccountRoot.create({ draft });
  }
});

// @filename: CreateOrder.tsx

export function OrderForm({
  order,
  onSave,
}: {
  order: Loaded<typeof BubbleTeaOrder> | Loaded<typeof DraftBubbleTeaOrder>;
  onSave?: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <form onSubmit={onSave}>
      <label>
        Name
        <input
          type="text"
          value={order.name}
          onChange={(e) => (order.name = e.target.value)}
          required
        />
      </label>

      {onSave && <button type="submit">Submit</button>}
    </form>
  );
}

// ---cut---
// CreateOrder.tsx
export function CreateOrder() {
  const { me } = useAccount(JazzAccount, { // [!code ++:5]
    resolve: { root: { draft: true } },
  });

  if (!me?.root) return;

  const onSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    const draft = me.root.draft; // [!code ++:2]
    if (!draft) return;

    const validation = DraftBubbleTeaOrder.validate(draft);
    if (validation.errors.length > 0) {
      console.log(validation.errors);
      return;
    }

    const order = draft as Loaded<typeof BubbleTeaOrder>;
    console.log("Order created:", order);

    // create a new empty draft
    me.root.draft = DraftBubbleTeaOrder.create( // [!code ++:3]
      {},
    );
  };

  return <CreateOrderForm id={me.root.draft.id} onSave={onSave} />
}

function CreateOrderForm({ // [!code ++:13]
  id,
  onSave,
}: {
  id: string
  onSave: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  const draft = useCoState(DraftBubbleTeaOrder, id);

  if (!draft) return;

  return <OrderForm order={draft} onSave={onSave} />;
}
```
</CodeGroup>

When the new draft is created, we need to call `useCoState` again, so that we are passing the new draft to `<OrderForm/>`.

There you have it! Notice that when you refresh the page, you will see your unsaved changes.


## Draft indicator

To improve the UX even further, in just a few more steps, we can tell the user that they currently have unsaved changes.

Simply add a `hasChanges` helper to your schema.

<CodeGroup>
```ts twoslash
// ---cut---
// schema.ts
export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
}).withHelpers((Self) => ({
  validate(draft: Loaded<typeof Self>) {
    const errors: string[] = [];

    if (!draft.name) {
      errors.push("Plese enter a name.");
    }

    return { errors };
  },

  hasChanges(draft?: Loaded<typeof Self>) { // [!code ++:3]
    return draft ? Object.keys(draft._edits).length : false;
  },
}));
```
</CodeGroup>

In the UI, you can choose how you want to show the draft indicator.

<CodeGroup>
```tsx twoslash

export const BubbleTeaOrder = co.map({
  name: z.string(),
});

export const DraftBubbleTeaOrder = co.map({
  name: z.optional(z.string()),
}).withHelpers((Self) => ({
  validate(draft: Loaded<typeof Self>) {
    const errors: string[] = [];

    if (!draft.name) {
      errors.push("Plese enter a name.");
    }

    return { errors };
  },

  hasChanges(draft?: Loaded<typeof Self>) { // [!code ++:3]
    return draft ? Object.keys(draft._edits).length : false;
  },
}));

export const AccountRoot = co.map({
  draft: DraftBubbleTeaOrder,
});

export const JazzAccount = co.account({
  root: AccountRoot,
  profile: co.map({ name: z.string() }),
}).withMigration((account, creationProps?: { name: string }) => {
  if (account.root === undefined) {
    const draft = DraftBubbleTeaOrder.create({});

    account.root = AccountRoot.create({ draft });
  }
});

// ---cut---
// DraftIndicator.tsx
export function DraftIndicator() {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { draft: true } },
  });

  if (DraftBubbleTeaOrder.hasChanges(me?.root.draft)) {
    return (
      <p>You have a draft</p>
    );
  }
}
```
</CodeGroup>

A more subtle way is to show a small dot next to the Create button.

<div className="not-prose border p-5 text-center">
  <button type="button" className="relative border rounded-md py-2 px-4 text-center shadow-sm">
    Create order
    <span
      title="You have a draft"
      className="absolute -top-1 -right-1 bg-blue-500 border-2 border-white w-3 h-3 rounded-full dark:border-stone-925"
    >
    </span>
  </button>
</div>

## Handling different types of data

Forms can be more complex than just a single string field, so we've put together an example app that shows you
how to handle single-select, multi-select, date, and boolean inputs.

[See the full example here.](https://github.com/garden-co/jazz/tree/main/examples/form)

<CodeGroup>
```tsx twoslash

export const BubbleTeaAddOnTypes = [
  "Pearl",
  "Lychee jelly",
  "Red bean",
  "Brown sugar",
  "Taro",
] as const;

export const ListOfBubbleTeaAddOns = co.list(
  z.literal([...BubbleTeaAddOnTypes]),
);

// ---cut---
// schema.ts
export const BubbleTeaOrder = co.map({
  baseTea: z.literal(["Black", "Oolong", "Jasmine", "Thai"]),
  addOns: ListOfBubbleTeaAddOns,
  deliveryDate: z.date(),
  withMilk: z.boolean(),
  instructions: z.optional(z.string()),
});
  ```
</CodeGroup>



#### Organization/Team

# How to share data between users through Organizations

This guide shows you how to share a set of CoValues between users. Different apps have different names for this concept, such as "teams" or "workspaces".

We'll use the term Organization.

[See the full example here.](https://github.com/garden-co/jazz/tree/main/examples/organization)

## Defining the schema for an Organization

Create a CoMap shared by the users of the same organization to act as a root (or "main database") for the shared data within an organization.

For this example, users within an `Organization` will be sharing `Project`s.

<CodeGroup>
```ts twoslash
// ---cut---
// schema.ts
export const Project = co.map({
  name: z.string(),
});

export const Organization = co.map({
  name: z.string(),

  // shared data between users of each organization
  projects: co.list(Project),
});

export const ListOfOrganizations = co.list(Organization);
```
</CodeGroup>

Learn more about [defining schemas](/docs/schemas/covalues).

## Adding a list of Organizations to the user's Account

Let's add the list of `Organization`s to the user's Account `root` so they can access them.

<CodeGroup>
```ts twoslash
export const Project = co.map({
  name: z.string(),
});

export const Organization = co.map({
  name: z.string(),

  // shared data between users of each organization
  projects: co.list(Project),
});

// ---cut---
// schema.ts
export const JazzAccountRoot = co.map({
  organizations: co.list(Organization),
});

export const JazzAccount = co
  .account({
    root: JazzAccountRoot,
    profile: co.profile({}),
  })
  .withMigration((account) => {
    if (account.root === undefined) {
      // Using a Group as an owner allows you to give access to other users
      const organizationGroup = Group.create();

      const organizations = co.list(Organization).create([
        // Create the first Organization so users can start right away
        Organization.create(
          {
            name: "My organization",
            projects: co.list(Project).create([], organizationGroup),
          },
          organizationGroup,
        ),
      ]);
      account.root = JazzAccountRoot.create({ organizations });
    }
  });
```
</CodeGroup>

This schema now allows users to create `Organization`s and add `Project`s to them.

[See the schema for the example app here.](https://github.com/garden-co/jazz/blob/main/examples/organization/src/schema.ts)

## Adding members to an Organization

Here are different ways to add members to an `Organization`.

- Send users an invite link.
- [The user requests to join.](/docs/groups/sharing#requesting-invites)

This guide and the example app show you the first method.

### Adding members through invite links

Here's how you can generate an [invite link](/docs/groups/sharing#invites).

When the user accepts the invite, add the `Organization` to the user's `organizations` list.

<CodeGroup>
```tsx twoslash

const Project = z.object({
  name: z.string(),
});

const Organization = co.map({
  name: z.string(),
  projects: co.list(Project),
});

const JazzAccountRoot = co.map({
  organizations: co.list(Organization),
});

const JazzAccount = co.account({
  root: JazzAccountRoot,
  profile: co.profile({}),
});

// ---cut---
export function AcceptInvitePage() {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { organizations: { $each: { $onError: null } } } },
  });

  const onAccept = (organizationId: string) => {
    if (me) {
      Organization.load(organizationId).then((organization) => {
        if (organization) {
          // avoid duplicates
          const ids = me.root.organizations.map(
            (organization) => organization?.id,
          );
          if (ids.includes(organizationId)) return;

          me.root.organizations.push(organization);
        }
      });
    }
  };

  useAcceptInvite({
    invitedObjectSchema: Organization,
    onAccept,
  });

  return <p>Accepting invite...</p>;
}
```
</CodeGroup>

## Further reading

- [Allowing users to request an invite to join a Group](/docs/groups/sharing#requesting-invites)
- [Groups as permission scopes](/docs/groups/intro#adding-group-members-by-id)



## Resources


- [Documentation](https://jazz.tools/docs): Detailed documentation about Jazz

- [Examples](https://jazz.tools/examples): Code examples and tutorials

## Music Example


### /vercel/path0/examples/music-player/src/1_schema.ts

```ts
import { co, z } from "jazz-tools";

/** Walkthrough: Defining the data model with CoJSON
 *
 *  Here, we define our main data model of tasks, lists of tasks and projects
 *  using CoJSON's collaborative map and list types, CoMap & CoList.
 *
 *  CoMap values and CoLists items can contain:
 *  - arbitrary immutable JSON
 *  - other CoValues
 **/

export const MusicTrackWaveform = co.map({
  data: z.array(z.number()),
});
export type MusicTrackWaveform = co.loaded<typeof MusicTrackWaveform>;

export const MusicTrack = co.map({
  /**
   *  Attributes are defined using zod schemas
   */
  title: z.string(),
  duration: z.number(),

  /**
   * You can define relations between coValues using the other CoValue schema
   * You can mark them optional using z.optional()
   */
  waveform: MusicTrackWaveform,

  /**
   * In Jazz you can upload files using FileStream.
   *
   * As for any other coValue the music files we put inside FileStream
   * is available offline and end-to-end encrypted 😉
   */
  file: co.fileStream(),

  isExampleTrack: z.optional(z.boolean()),

  /**
   * You can use getters for recusrive relations
   */
  get sourceTrack(): z.ZodOptional<typeof MusicTrack> {
    return z.optional(MusicTrack);
  },
});
export type MusicTrack = co.loaded<typeof MusicTrack>;

export const Playlist = co.map({
  title: z.string(),
  tracks: co.list(MusicTrack), // CoList is the collaborative version of Array
});
export type Playlist = co.loaded<typeof Playlist>;
/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const MusicaAccountRoot = co.map({
  // The root playlist works as container for the tracks that
  // the user has uploaded
  rootPlaylist: Playlist,
  // Here we store the list of playlists that the user has created
  // or that has been invited to
  playlists: co.list(Playlist),
  // We store the active track and playlist as coValue here
  // so when the user reloads the page can see the last played
  // track and playlist
  // You can also add the position in time if you want make it possible
  // to resume the song
  activeTrack: z.optional(MusicTrack),
  activePlaylist: Playlist,

  exampleDataLoaded: z.optional(z.boolean()),
});
export type MusicaAccountRoot = co.loaded<typeof MusicaAccountRoot>;
export const MusicaAccount = co
  .account({
    /** the default user profile with a name */
    profile: co.profile(),
    root: MusicaAccountRoot,
  })
  .withMigration((account) => {
    /**
     *  The account migration is run on account creation and on every log-in.
     *  You can use it to set up the account root and any other initial CoValues you need.
     */
    if (account.root === undefined) {
      const tracks = co.list(MusicTrack).create([]);
      const rootPlaylist = Playlist.create({
        tracks,
        title: "",
      });

      account.root = MusicaAccountRoot.create({
        rootPlaylist,
        playlists: co.list(Playlist).create([]),
        activeTrack: undefined,
        activePlaylist: rootPlaylist,
        exampleDataLoaded: false,
      });
    }
  });
export type MusicaAccount = co.loaded<typeof MusicaAccount>;

/** Walkthrough: Continue with ./2_main.tsx */

```

### /vercel/path0/examples/music-player/src/2_main.tsx

```ts
import { Toaster } from "@/components/ui/toaster";
import { JazzInspector } from "jazz-tools/inspector";
/* eslint-disable react-refresh/only-export-components */
import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider, createHashRouter } from "react-router-dom";
import { HomePage } from "./3_HomePage";
import { useMediaPlayer } from "./5_useMediaPlayer";
import { InvitePage } from "./6_InvitePage";
import "./index.css";

import { MusicaAccount } from "@/1_schema";
import { apiKey } from "@/apiKey.ts";
import { SidebarProvider } from "@/components/ui/sidebar";
import { JazzProvider } from "jazz-react";
import { onAnonymousAccountDiscarded } from "./4_actions";
import { useUploadExampleData } from "./lib/useUploadExampleData";

/**
 * Walkthrough: The top-level provider `<JazzProvider/>`
 *
 * This shows how to use the top-level provider `<JazzProvider/>`,
 * which provides the rest of the app with a controlled account (used through `useAccount` later).
 * Here we use `DemoAuth` which is great for prototyping you app without wasting time on figuring out
 * the best way to do auth.
 *
 * `<JazzProvider/>` also runs our account migration
 */

function Main() {
  const mediaPlayer = useMediaPlayer();

  useUploadExampleData();

  const router = createHashRouter([
    {
      path: "/",
      element: <HomePage mediaPlayer={mediaPlayer} />,
    },
    {
      path: "/playlist/:playlistId",
      element: <HomePage mediaPlayer={mediaPlayer} />,
    },
    {
      path: "/invite/*",
      element: <InvitePage />,
    },
  ]);

  return (
    <>
      <RouterProvider router={router} />
      {/* <PlayerControls mediaPlayer={mediaPlayer} /> */}
      <Toaster />
    </>
  );
}

const peer =
  (new URL(window.location.href).searchParams.get(
    "peer",
  ) as `ws://${string}`) ?? `wss://cloud.jazz.tools/?key=${apiKey}`;

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <JazzProvider
      sync={{
        peer,
      }}
      storage="indexedDB"
      AccountSchema={MusicaAccount}
      defaultProfileName="Anonymous unicorn"
      onAnonymousAccountDiscarded={onAnonymousAccountDiscarded}
    >
      <SidebarProvider>
        <Main />
        <JazzInspector />
      </SidebarProvider>
    </JazzProvider>
  </React.StrictMode>,
);

```

### /vercel/path0/examples/music-player/src/3_HomePage.tsx

```ts
import { useToast } from "@/hooks/use-toast";
import {
  createInviteLink,
  useAccount,
  useCoState,
  useIsAuthenticated,
} from "jazz-react";
import { useParams } from "react-router";
import { MusicaAccount, Playlist } from "./1_schema";
import { uploadMusicTracks } from "./4_actions";
import { MediaPlayer } from "./5_useMediaPlayer";
import { FileUploadButton } from "./components/FileUploadButton";
import { MusicTrackRow } from "./components/MusicTrackRow";
import { PlaylistTitleInput } from "./components/PlaylistTitleInput";
import { SidePanel } from "./components/SidePanel";
import { Button } from "./components/ui/button";
import { SidebarInset, SidebarTrigger } from "./components/ui/sidebar";
import { usePlayState } from "./lib/audio/usePlayState";

export function HomePage({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  /**
   * `me` represents the current user account, which will determine
   *  access rights to CoValues. We get it from the top-level provider `<WithJazz/>`.
   */
  const { me } = useAccount(MusicaAccount, {
    resolve: { root: { rootPlaylist: true, playlists: true } },
  });

  const playState = usePlayState();
  const isPlaying = playState.value === "play";
  const { toast } = useToast();

  async function handleFileLoad(files: FileList) {
    /**
     * Follow this function definition to see how we update
     * values in Jazz and manage files!
     */
    await uploadMusicTracks(files);
  }

  const params = useParams<{ playlistId: string }>();
  const playlistId = params.playlistId ?? me?.root._refs.rootPlaylist.id;

  const playlist = useCoState(Playlist, playlistId, {
    resolve: { tracks: true },
  });

  const isRootPlaylist = !params.playlistId;
  const isPlaylistOwner = playlist?._owner.myRole() === "admin";
  const isActivePlaylist = playlistId === me?.root.activePlaylist?.id;

  const handlePlaylistShareClick = async () => {
    if (!isPlaylistOwner) return;

    const inviteLink = createInviteLink(playlist, "reader");

    await navigator.clipboard.writeText(inviteLink);

    toast({
      title: "Invite link copied into the clipboard",
    });
  };

  const isAuthenticated = useIsAuthenticated();

  return (
    <SidebarInset className="flex flex-col h-screen text-gray-800 bg-blue-50">
      <div className="flex flex-1 overflow-hidden">
        <SidePanel mediaPlayer={mediaPlayer} />
        <main className="flex-1 p-6 overflow-y-auto">
          <SidebarTrigger />
          <div className="flex items-center justify-between mb-6">
            {isRootPlaylist ? (
              <h1 className="text-2xl font-bold text-blue-800">All tracks</h1>
            ) : (
              <PlaylistTitleInput playlistId={playlistId} />
            )}
            <div className="flex items-center space-x-4">
              {isRootPlaylist && (
                <>
                  <FileUploadButton onFileLoad={handleFileLoad}>
                    Add file
                  </FileUploadButton>
                </>
              )}
              {!isRootPlaylist && isAuthenticated && (
                <Button onClick={handlePlaylistShareClick}>
                  Share playlist
                </Button>
              )}
            </div>
          </div>
          <ul className="flex flex-col">
            {playlist?.tracks?.map(
              (track) =>
                track && (
                  <MusicTrackRow
                    trackId={track.id}
                    key={track.id}
                    isLoading={mediaPlayer.loading === track.id}
                    isPlaying={
                      mediaPlayer.activeTrackId === track.id &&
                      isActivePlaylist &&
                      isPlaying
                    }
                    onClick={() => {
                      mediaPlayer.setActiveTrack(track, playlist);
                    }}
                    showAddToPlaylist={isRootPlaylist}
                  />
                ),
            )}
          </ul>
        </main>
      </div>
    </SidebarInset>
  );
}

```

### /vercel/path0/examples/music-player/src/4_actions.ts

```ts
import { getAudioFileData } from "@/lib/audio/getAudioFileData";
import { FileStream, Group, co } from "jazz-tools";
import {
  MusicTrack,
  MusicTrackWaveform,
  MusicaAccount,
  Playlist,
} from "./1_schema";

/**
 * Walkthrough: Actions
 *
 * With Jazz is very simple to update the state, you
 * just mutate the values and we take care of triggering
 * the updates and sync  and persist the values you change.
 *
 * We have grouped the complex updates here in an actions file
 * just to keep them separated from the components.
 *
 * Jazz is very unopinionated in this sense and you can adopt the
 * pattern that best fits your app.
 */

export async function uploadMusicTracks(
  files: Iterable<File>,
  isExampleTrack: boolean = false,
) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
      },
    },
  });

  for (const file of files) {
    // The ownership object defines the user that owns the created coValues
    // We are creating a group for each CoValue in order to be able to share them via Playlist
    const group = Group.create();

    const data = await getAudioFileData(file);

    // We transform the file blob into a FileStream
    // making it a collaborative value that is encrypted, easy
    // to share across devices and users and available offline!
    const fileStream = await FileStream.createFromBlob(file, group);

    const musicTrack = MusicTrack.create(
      {
        file: fileStream,
        duration: data.duration,
        waveform: MusicTrackWaveform.create({ data: data.waveform }, group),
        title: file.name,
        isExampleTrack,
      },
      group,
    );

    // The newly created musicTrack can be associated to the
    // user track list using a simple push call
    root.rootPlaylist.tracks.push(musicTrack);
  }
}

export async function createNewPlaylist() {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        playlists: true,
      },
    },
  });

  // Since playlists are meant to be shared we associate them
  // to a group which will contain the keys required to get
  // access to the "owned" values
  const playlistGroup = Group.create();

  const playlist = Playlist.create(
    {
      title: "New Playlist",
      tracks: co.list(MusicTrack).create([], playlistGroup),
    },
    playlistGroup,
  );

  // Again, we associate the new playlist to the
  // user by pushing it into the playlists CoList
  root.playlists.push(playlist);

  return playlist;
}

export async function addTrackToPlaylist(
  playlist: Playlist,
  track: MusicTrack,
) {
  const alreadyAdded = playlist.tracks?.some(
    (t) => t?.id === track.id || t?._refs.sourceTrack?.id === track.id,
  );

  if (alreadyAdded) return;

  // Check if the track has been created after the Group inheritance was introduced
  if (track._owner._type === "Group" && playlist._owner._type === "Group") {
    /**
     * Extending the track with the Playlist group in order to make the music track
     * visible to the Playlist user
     */
    const trackGroup = track._owner;
    trackGroup.extend(playlist._owner);

    playlist.tracks?.push(track);
    return;
  }
}

export async function removeTrackFromPlaylist(
  playlist: Playlist,
  track: MusicTrack,
) {
  const notAdded = !playlist.tracks?.some(
    (t) => t?.id === track.id || t?._refs.sourceTrack?.id === track.id,
  );

  if (notAdded) return;

  if (track._owner._type === "Group" && playlist._owner._type === "Group") {
    const trackGroup = track._owner;
    await trackGroup.revokeExtend(playlist._owner);

    const index =
      playlist.tracks?.findIndex(
        (t) => t?.id === track.id || t?._refs.sourceTrack?.id === track.id,
      ) ?? -1;
    if (index > -1) {
      playlist.tracks?.splice(index, 1);
    }
    return;
  }
}

export async function updatePlaylistTitle(playlist: Playlist, title: string) {
  playlist.title = title;
}

export async function updateMusicTrackTitle(track: MusicTrack, title: string) {
  track.title = title;
}

export async function updateActivePlaylist(playlist?: Playlist) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        activePlaylist: true,
        rootPlaylist: true,
      },
    },
  });

  root.activePlaylist = playlist ?? root.rootPlaylist;
}

export async function updateActiveTrack(track: MusicTrack) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {},
    },
  });

  root.activeTrack = track;
}

export async function onAnonymousAccountDiscarded(
  anonymousAccount: MusicaAccount,
) {
  const { root: anonymousAccountRoot } = await anonymousAccount.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: {
            $each: true,
          },
        },
      },
    },
  });

  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
      },
    },
  });

  for (const track of anonymousAccountRoot.rootPlaylist.tracks) {
    if (track.isExampleTrack) continue;

    const trackGroup = track._owner.castAs(Group);
    trackGroup.addMember(me, "admin");

    me.root.rootPlaylist.tracks.push(track);
  }
}

export async function deletePlaylist(playlistId: string) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        playlists: true,
      },
    },
  });

  const index = root.playlists.findIndex((p) => p?.id === playlistId);
  if (index > -1) {
    root.playlists.splice(index, 1);
  }
}

```

### /vercel/path0/examples/music-player/src/5_useMediaPlayer.ts

```ts
import { MusicTrack, MusicaAccount, Playlist } from "@/1_schema";
import { usePlayMedia } from "@/lib/audio/usePlayMedia";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useAccount } from "jazz-react";
import { FileStream } from "jazz-tools";
import { useRef, useState } from "react";
import { updateActivePlaylist, updateActiveTrack } from "./4_actions";
import { getNextTrack, getPrevTrack } from "./lib/getters";

export function useMediaPlayer() {
  const { me } = useAccount(MusicaAccount, {
    resolve: { root: true },
  });

  const playState = usePlayState();
  const playMedia = usePlayMedia();

  const [loading, setLoading] = useState<string | null>(null);

  const activeTrackId = me?.root._refs.activeTrack?.id;

  // Reference used to avoid out-of-order track loads
  const lastLoadedTrackId = useRef<string | null>(null);

  async function loadTrack(track: MusicTrack) {
    lastLoadedTrackId.current = track.id;

    setLoading(track.id);

    const file = await FileStream.loadAsBlob(track._refs.file!.id); // TODO: see if we can avoid !

    if (!file) {
      setLoading(null);
      return;
    }

    // Check if another track has been loaded during
    // the file download
    if (lastLoadedTrackId.current !== track.id) {
      return;
    }

    updateActiveTrack(track);

    await playMedia(file);

    setLoading(null);
  }

  async function playNextTrack() {
    const track = await getNextTrack();

    if (track) {
      updateActiveTrack(track);
      await loadTrack(track);
    }
  }

  async function playPrevTrack() {
    const track = await getPrevTrack();

    if (track) {
      await loadTrack(track);
    }
  }

  async function setActiveTrack(track: MusicTrack, playlist?: Playlist) {
    if (activeTrackId === track.id && lastLoadedTrackId.current !== null) {
      playState.toggle();
      return;
    }

    updateActivePlaylist(playlist);

    await loadTrack(track);

    if (playState.value === "pause") {
      playState.toggle();
    }
  }

  return {
    activeTrackId,
    setActiveTrack,
    playNextTrack,
    playPrevTrack,
    loading,
  };
}

export type MediaPlayer = ReturnType<typeof useMediaPlayer>;

```

### /vercel/path0/examples/music-player/src/6_InvitePage.tsx

```ts
import { useAcceptInvite, useIsAuthenticated } from "jazz-react";
import { useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { MusicaAccount, Playlist } from "./1_schema";

export function InvitePage() {
  const navigate = useNavigate();

  const isAuthenticated = useIsAuthenticated();

  useAcceptInvite({
    invitedObjectSchema: Playlist,
    onAccept: useCallback(
      async (playlistId: string) => {
        const playlist = await Playlist.load(playlistId, {});

        const me = await MusicaAccount.getMe().ensureLoaded({
          resolve: {
            root: {
              playlists: true,
            },
          },
        });

        if (
          playlist &&
          !me.root.playlists.some((item) => playlist.id === item?.id)
        ) {
          me.root.playlists.push(playlist);
        }

        navigate("/playlist/" + playlistId);
      },
      [navigate],
    ),
  });

  return isAuthenticated ? (
    <p>Accepting invite....</p>
  ) : (
    <p>Please sign in to accept the invite.</p>
  );
}

```

### /vercel/path0/examples/music-player/src/apiKey.ts

```ts
export const apiKey = "music-player-example-jazz@garden.co";

```

### /vercel/path0/examples/music-player/src/components/AuthButton.tsx

```ts
"use client";

import { Button } from "@/components/ui/button";
import { useAccount, useIsAuthenticated } from "jazz-react";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { AuthModal } from "./AuthModal";

export function AuthButton() {
  const [open, setOpen] = useState(false);
  const { logOut } = useAccount();
  const navigate = useNavigate();

  const isAuthenticated = useIsAuthenticated();

  function handleSignOut() {
    logOut();
    navigate("/");
  }

  if (isAuthenticated) {
    return (
      <Button variant="ghost" onClick={handleSignOut}>
        Sign out
      </Button>
    );
  }

  return (
    <>
      <Button onClick={() => setOpen(true)} variant="ghost">
        Sign up
      </Button>
      <AuthModal open={open} onOpenChange={setOpen} />
    </>
  );
}

```

### /vercel/path0/examples/music-player/src/components/AuthModal.tsx

```ts
import { MusicaAccount } from "@/1_schema";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useAccount, usePasskeyAuth } from "jazz-react";
import { useState } from "react";

interface AuthModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [username, setUsername] = useState("");
  const [isSignUp, setIsSignUp] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const { me } = useAccount(MusicaAccount, {
    resolve: {
      root: {
        rootPlaylist: {
          tracks: {
            $each: true,
          },
        },
      },
    },
  });

  const auth = usePasskeyAuth({
    appName: "Jazz Music Player",
  });

  const handleViewChange = () => {
    setIsSignUp(!isSignUp);
    setError(null);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    try {
      if (isSignUp) {
        await auth.signUp(username);
      } else {
        await auth.logIn();
      }
      onOpenChange(false);
    } catch (error) {
      setError(error instanceof Error ? error.message : "Unknown error");
    }
  };

  const shouldShowTransferRootPlaylist =
    !isSignUp &&
    me?.root.rootPlaylist.tracks.some((track) => !track.isExampleTrack);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="text-2xl font-bold">
            {isSignUp ? "Create account" : "Welcome back"}
          </DialogTitle>
          <DialogDescription>
            {isSignUp
              ? "Sign up to enable network sync and share your playlists with others"
              : "Changes done before logging in will be lost"}
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          {isSignUp && (
            <div className="space-y-2">
              <Label htmlFor="username">Username</Label>
              <Input
                id="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Enter your username"
                required
              />
            </div>
          )}
          {error && <div className="text-sm text-red-500">{error}</div>}
          {shouldShowTransferRootPlaylist && (
            <div className="text-sm text-red-500">
              You have tracks in your root playlist that are not example tracks.
              If you log in with a passkey, your playlists will be transferred
              to your logged account.
            </div>
          )}
          <div className="space-y-4">
            <Button
              type="submit"
              className="w-full bg-blue-600 hover:bg-blue-700"
            >
              {isSignUp ? "Sign up with passkey" : "Login with passkey"}
            </Button>
            <div className="text-center text-sm">
              {isSignUp ? "Already have an account?" : "Don't have an account?"}{" "}
              <button
                type="button"
                onClick={handleViewChange}
                className="text-blue-600 hover:underline"
              >
                {isSignUp ? "Login" : "Sign up"}
              </button>
            </div>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}

```

### /vercel/path0/examples/music-player/src/components/FileUploadButton.tsx

```ts
import { ReactNode } from "react";
import { Button } from "./ui/button";

export function FileUploadButton(props: {
  onFileLoad: (files: FileList) => Promise<void>;
  children: ReactNode;
}) {
  async function handleFileLoad(evt: React.ChangeEvent<HTMLInputElement>) {
    if (!evt.target.files) return;

    await props.onFileLoad(evt.target.files);

    evt.target.value = "";
  }

  return (
    <Button>
      <label className="flex items-center  cursor-pointer p-2">
        <input type="file" onChange={handleFileLoad} multiple hidden />
        {props.children}
      </label>
    </Button>
  );
}

```

### /vercel/path0/examples/music-player/src/components/LocalOnlyTag.tsx

```ts
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useIsAuthenticated } from "jazz-react";
import { Info } from "lucide-react";

export function LocalOnlyTag() {
  const isAuthenticated = useIsAuthenticated();

  if (isAuthenticated) {
    return null;
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="inline-flex items-center gap-1.5 cursor-help">
            <Badge variant="default" className="h-5 text-xs font-normal">
              Local only
            </Badge>
            <Info className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-[250px]">
          <p>
            Sign up to enable network sync and share your playlists with others
          </p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

```

### /vercel/path0/examples/music-player/src/components/LogoutButton.tsx

```ts
import { useAccount } from "jazz-react";
import { Button } from "./ui/button";

export function LogoutButton() {
  const { logOut } = useAccount();

  return <Button onClick={logOut}>Logout</Button>;
}

```

### /vercel/path0/examples/music-player/src/components/MusicTrackRow.tsx

```ts
import { MusicTrack, MusicaAccount, Playlist } from "@/1_schema";
import { addTrackToPlaylist, removeTrackFromPlaylist } from "@/4_actions";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import { useAccount, useCoState } from "jazz-react";
import { Loaded } from "jazz-tools";
import { MoreHorizontal } from "lucide-react";
import { Fragment } from "react/jsx-runtime";
import { MusicTrackTitleInput } from "./MusicTrackTitleInput";
import { Button } from "./ui/button";

export function MusicTrackRow({
  trackId,
  isLoading,
  isPlaying,
  onClick,
  showAddToPlaylist,
}: {
  trackId: string;
  isLoading: boolean;
  isPlaying: boolean;
  onClick: (track: Loaded<typeof MusicTrack>) => void;
  showAddToPlaylist: boolean;
}) {
  const track = useCoState(MusicTrack, trackId);

  const { me } = useAccount(MusicaAccount, {
    resolve: { root: { playlists: { $each: true } } },
  });

  const playlists = me?.root.playlists ?? [];

  function handleTrackClick() {
    if (!track) return;
    onClick(track);
  }

  function handleAddToPlaylist(playlist: Loaded<typeof Playlist>) {
    if (!track) return;
    addTrackToPlaylist(playlist, track);
  }

  function handleRemoveFromPlaylist(playlist: Loaded<typeof Playlist>) {
    if (!track) return;
    removeTrackFromPlaylist(playlist, track);
  }

  function deleteTrack() {
    if (!me || !track) return;
    const tracks = me.root.rootPlaylist?.tracks;
    if (!tracks) return;
    const index = tracks.findIndex((t) => t?.id === trackId);
    if (index !== -1) {
      tracks.splice(index, 1);
    }
  }

  return (
    <li
      className={
        "flex gap-1  hover:bg-slate-200 group py-2 px-2 cursor-pointer"
      }
      onClick={handleTrackClick}
    >
      <button
        className={cn(
          "flex items-center justify-center bg-transparent w-8 h-8 ",
          !isPlaying && "group-hover:bg-slate-300 rounded-full",
        )}
        onClick={handleTrackClick}
        aria-label={`${isPlaying ? "Pause" : "Play"} ${track?.title}`}
      >
        {isLoading ? (
          <div className="animate-spin">߷</div>
        ) : isPlaying ? (
          "⏸️"
        ) : (
          "▶️"
        )}
      </button>
      <MusicTrackTitleInput trackId={trackId} />
      <div onClick={(evt) => evt.stopPropagation()}>
        {showAddToPlaylist && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant="ghost"
                className="h-8 w-8 p-0"
                aria-label={`Open ${track?.title} menu`}
              >
                <span className="sr-only">Open menu</span>
                <MoreHorizontal className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem
                key={`delete`}
                onSelect={async () => {
                  if (!track) return;
                  deleteTrack();
                }}
              >
                Delete
              </DropdownMenuItem>
              {playlists.map((playlist, index) => (
                <Fragment key={index}>
                  <DropdownMenuItem
                    key={`add-${index}`}
                    onSelect={() => handleAddToPlaylist(playlist)}
                  >
                    Add to {playlist.title}
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    key={`remove-${index}`}
                    onSelect={() => handleRemoveFromPlaylist(playlist)}
                  >
                    Remove from {playlist.title}
                  </DropdownMenuItem>
                </Fragment>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>
    </li>
  );
}

```

### /vercel/path0/examples/music-player/src/components/MusicTrackTitleInput.tsx

```ts
import { MusicTrack } from "@/1_schema";
import { updateMusicTrackTitle } from "@/4_actions";
import { useCoState } from "jazz-react";
import { ChangeEvent, useState } from "react";

export function MusicTrackTitleInput({
  trackId,
}: {
  trackId: string | undefined;
}) {
  const track = useCoState(MusicTrack, trackId);
  const [isEditing, setIsEditing] = useState(false);
  const [localTrackTitle, setLocalTrackTitle] = useState("");

  function handleTitleChange(evt: ChangeEvent<HTMLInputElement>) {
    setLocalTrackTitle(evt.target.value);
  }

  function handleFoucsIn() {
    setIsEditing(true);
    setLocalTrackTitle(track?.title ?? "");
  }

  function handleFocusOut() {
    setIsEditing(false);
    setLocalTrackTitle("");
    track && updateMusicTrackTitle(track, localTrackTitle);
  }

  const inputValue = isEditing ? localTrackTitle : (track?.title ?? "");

  return (
    <div
      className="relative flex-grow"
      onClick={(evt) => evt.stopPropagation()}
    >
      <input
        className="absolute w-full h-full left-0 bg-transparent px-1"
        value={inputValue}
        onChange={handleTitleChange}
        spellCheck="false"
        onFocus={handleFoucsIn}
        onBlur={handleFocusOut}
        aria-label={`Edit track title: ${track?.title}`}
      />
      <span className="opacity-0 px-1 w-fit pointer-events-none whitespace-pre">
        {inputValue}
      </span>
    </div>
  );
}

```

### /vercel/path0/examples/music-player/src/components/PlayerControls.tsx

```ts
import { MusicTrack, MusicaAccount } from "@/1_schema";
import { MediaPlayer } from "@/5_useMediaPlayer";
import { useMediaEndListener } from "@/lib/audio/useMediaEndListener";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useKeyboardListener } from "@/lib/useKeyboardListener";
import { useAccount, useCoState } from "jazz-react";
import { Pause, Play, SkipBack, SkipForward } from "lucide-react";
import { Waveform } from "./Waveform";

export function PlayerControls({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const playState = usePlayState();
  const isPlaying = playState.value === "play";

  const activePlaylist = useAccount(MusicaAccount, {
    resolve: { root: { activePlaylist: true } },
  }).me?.root.activePlaylist;

  useMediaEndListener(mediaPlayer.playNextTrack);
  useKeyboardListener("Space", () => {
    if (document.activeElement !== document.body) return;

    playState.toggle();
  });

  const activeTrack = useCoState(MusicTrack, mediaPlayer.activeTrackId, {
    resolve: { waveform: true },
  });

  if (!activeTrack) return null;

  const activeTrackTitle = activeTrack.title;

  return (
    <footer className="flex items-center justify-between p-4 gap-4 bg-white border-t border-gray-200 fixed bottom-0 left-0 right-0 w-full">
      <div className="flex justify-center items-center space-x-2">
        <div className="flex items-center space-x-4">
          <button
            onClick={mediaPlayer.playPrevTrack}
            className="text-blue-600 hover:text-blue-800"
            aria-label="Previous track"
          >
            <SkipBack size={20} />
          </button>
          <button
            onClick={playState.toggle}
            className="w-[42px] h-[42px] flex items-center justify-center bg-blue-600 rounded-full text-white hover:bg-blue-700"
            aria-label={isPlaying ? "Pause active track" : "Play active track"}
          >
            {isPlaying ? (
              <Pause size={24} fill="currentColor" />
            ) : (
              <Play size={24} fill="currentColor" />
            )}
          </button>
          <button
            onClick={mediaPlayer.playNextTrack}
            className="text-blue-600 hover:text-blue-800"
            aria-label="Next track"
          >
            <SkipForward size={20} />
          </button>
        </div>
      </div>
      <div className=" sm:hidden md:flex flex-col flex-shrink-1 items-center w-[75%]">
        <Waveform track={activeTrack} height={30} />
      </div>
      <div className="flex flex-col items-end  gap-1 text-right min-w-fit w-[25%]">
        <h4 className="font-medium text-blue-800">{activeTrackTitle}</h4>
        <p className="text-sm text-gray-600">
          {activePlaylist?.title || "All tracks"}
        </p>
      </div>
    </footer>
  );
}

```

### /vercel/path0/examples/music-player/src/components/PlaylistTitleInput.tsx

```ts
import { Playlist } from "@/1_schema";
import { updatePlaylistTitle } from "@/4_actions";
import { useCoState } from "jazz-react";
import { ChangeEvent, useState } from "react";

export function PlaylistTitleInput({
  playlistId,
}: {
  playlistId: string | undefined;
}) {
  const playlist = useCoState(Playlist, playlistId);
  const [isEditing, setIsEditing] = useState(false);
  const [localPlaylistTitle, setLocalPlaylistTitle] = useState("");

  function handleTitleChange(evt: ChangeEvent<HTMLInputElement>) {
    setLocalPlaylistTitle(evt.target.value);
  }

  function handleFoucsIn() {
    setIsEditing(true);
    setLocalPlaylistTitle(playlist?.title ?? "");
  }

  function handleFocusOut() {
    setIsEditing(false);
    setLocalPlaylistTitle("");
    playlist && updatePlaylistTitle(playlist, localPlaylistTitle);
  }

  const inputValue = isEditing ? localPlaylistTitle : (playlist?.title ?? "");

  return (
    <input
      value={inputValue}
      onChange={handleTitleChange}
      className="text-2xl font-bold text-blue-800 bg-transparent"
      onFocus={handleFoucsIn}
      onBlur={handleFocusOut}
      aria-label={`Playlist title`}
    />
  );
}

```

### /vercel/path0/examples/music-player/src/components/SidePanel.tsx

```ts
import { MusicTrack, MusicaAccount } from "@/1_schema";
import { createNewPlaylist, deletePlaylist } from "@/4_actions";
import { MediaPlayer } from "@/5_useMediaPlayer";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuAction,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useAccount, useCoState } from "jazz-react";
import { Home, Music, Pause, Play, Plus, Trash2 } from "lucide-react";
import { useNavigate, useParams } from "react-router";
import { AuthButton } from "./AuthButton";

export function SidePanel({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const { playlistId } = useParams();
  const navigate = useNavigate();
  const { me } = useAccount(MusicaAccount, {
    resolve: { root: { playlists: { $each: true } } },
  });

  const playState = usePlayState();
  const isPlaying = playState.value === "play";

  function handleAllTracksClick() {
    navigate(`/`);
  }

  function handlePlaylistClick(playlistId: string) {
    navigate(`/playlist/${playlistId}`);
  }

  async function handleDeletePlaylist(playlistId: string) {
    if (confirm("Are you sure you want to delete this playlist?")) {
      await deletePlaylist(playlistId);
      navigate(`/`);
    }
  }

  async function handleCreatePlaylist() {
    const playlist = await createNewPlaylist();
    navigate(`/playlist/${playlist.id}`);
  }

  const activeTrack = useCoState(MusicTrack, mediaPlayer.activeTrackId, {
    resolve: { waveform: true },
  });

  const activeTrackTitle = activeTrack?.title;

  return (
    <Sidebar>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem className="flex items-center gap-2">
            <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-blue-600 text-white">
              <svg
                className="size-4"
                viewBox="0 0 24 24"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path
                  d="M9 18V5l12-2v13"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
                <path
                  d="M6 15H3c-1.1 0-2 .9-2 2v4c0 1.1.9 2 2 2h3c1.1 0 2-.9 2-2v-4c0-1.1-.9-2-2-2zM18 13h-3c-1.1 0-2 .9-2 2v4c0 1.1.9 2 2 2h3c1.1 0 2-.9 2-2v-4c0-1.1-.9-2-2-2z"
                  fill="currentColor"
                />
              </svg>
            </div>
            <div className="grid flex-1 text-left text-sm leading-tight">
              <span className="truncate font-semibold">Music Player</span>
            </div>
            <AuthButton />
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton onClick={handleAllTracksClick}>
                  <Home className="size-4" />
                  <span>Go to all tracks</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupLabel>Playlists</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton onClick={handleCreatePlaylist}>
                  <Plus className="size-4" />
                  <span>Add a new playlist</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
              {me?.root.playlists.map((playlist) => (
                <SidebarMenuItem key={playlist.id}>
                  <SidebarMenuButton
                    onClick={() => handlePlaylistClick(playlist.id)}
                    isActive={playlist.id === playlistId}
                  >
                    <div className="flex items-center gap-2">
                      <Music className="size-4" />
                      <span>{playlist.title}</span>
                    </div>
                  </SidebarMenuButton>
                  {playlist.id === playlistId && (
                    <SidebarMenuAction
                      onClick={() => handleDeletePlaylist(playlist.id)}
                      className="text-red-600 hover:text-red-800"
                    >
                      <Trash2 className="size-4" />
                      <span className="sr-only">Delete {playlist.title}</span>
                    </SidebarMenuAction>
                  )}
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      {activeTrack && (
        <SidebarFooter>
          <SidebarMenu>
            <SidebarMenuItem className="flex justify-end">
              <SidebarMenuButton
                onClick={playState.toggle}
                aria-label={
                  isPlaying ? "Pause active track" : "Play active track"
                }
              >
                <div className="w-[28px] h-[28px] flex items-center justify-center bg-blue-600 rounded-full text-white hover:bg-blue-700">
                  {isPlaying ? (
                    <Pause size={16} fill="currentColor" />
                  ) : (
                    <Play size={16} fill="currentColor" />
                  )}
                </div>
                <span>{activeTrackTitle}</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarFooter>
      )}
    </Sidebar>
  );
}

```

### /vercel/path0/examples/music-player/src/components/Waveform.tsx

```ts
import { MusicTrack, MusicTrackWaveform } from "@/1_schema";
import { usePlayerCurrentTime } from "@/lib/audio/usePlayerCurrentTime";
import { cn } from "@/lib/utils";
import { useCoState } from "jazz-react";
import { Loaded } from "jazz-tools";

export function Waveform(props: {
  track: Loaded<typeof MusicTrack>;
  height: number;
}) {
  const { track, height } = props;
  const waveformData = useCoState(
    MusicTrackWaveform,
    track._refs.waveform?.id,
  )?.data;
  const duration = track.duration;

  const currentTime = usePlayerCurrentTime();

  if (!waveformData) {
    return (
      <div
        style={{
          height,
        }}
      />
    );
  }

  const barCount = waveformData.length;
  const activeBar = Math.ceil(barCount * (currentTime.value / duration));

  function seek(i: number) {
    currentTime.setValue((i / barCount) * duration);
  }

  return (
    <div
      className="flex justify-center items-end w-full"
      style={{
        height,
        gap: 1,
      }}
    >
      {waveformData.map((value, i) => (
        <button
          type="button"
          key={i}
          onClick={() => seek(i)}
          className={cn(
            "w-1 transition-colors rounded-none rounded-t-lg min-h-1",
            activeBar >= i ? "bg-gray-500" : "bg-gray-300",
            "hover:bg-black hover:border-1 hover:border-solid hover:border-black",
            "focus-visible:outline-black focus:outline-none",
          )}
          style={{
            height: height * value,
          }}
          aria-label={`Seek to ${(i / barCount) * duration} seconds`}
        />
      ))}
    </div>
  );
}

```

### /vercel/path0/examples/music-player/src/components/ui/badge.tsx

```ts
import { type VariantProps, cva } from "class-variance-authority";
import * as React from "react";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2",
  {
    variants: {
      variant: {
        default:
          "border-transparent bg-primary text-primary-foreground hover:bg-primary/80",
        secondary:
          "border-transparent bg-secondary text-secondary-foreground hover:bg-secondary/80",
        destructive:
          "border-transparent bg-destructive text-destructive-foreground hover:bg-destructive/80",
        outline: "text-foreground",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  );
}

export { Badge, badgeVariants };

```

### /vercel/path0/examples/music-player/src/components/ui/button.tsx

```ts
import { Slot } from "@radix-ui/react-slot";
import { type VariantProps, cva } from "class-variance-authority";
import * as React from "react";

import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground hover:bg-primary/90",
        destructive:
          "bg-destructive text-destructive-foreground hover:bg-destructive/90",
        outline:
          "border border-input bg-background hover:bg-accent hover:text-accent-foreground",
        secondary:
          "bg-secondary text-secondary-foreground hover:bg-secondary/80",
        ghost: "hover:bg-accent hover:text-accent-foreground",
        link: "text-primary underline-offset-4 hover:underline",
      },
      size: {
        default: "h-10 px-4 py-2",
        sm: "h-9 rounded-md px-3",
        lg: "h-11 rounded-md px-8",
        icon: "h-10 w-10",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : "button";
    return (
      <Comp
        className={cn(buttonVariants({ variant, size, className }))}
        ref={ref}
        {...props}
      />
    );
  },
);
Button.displayName = "Button";

export { Button, buttonVariants };

```

### /vercel/path0/examples/music-player/src/components/ui/dialog.tsx

```ts
import * as DialogPrimitive from "@radix-ui/react-dialog";
import { X } from "lucide-react";
import * as React from "react";

import { cn } from "@/lib/utils";

const Dialog = DialogPrimitive.Root;

const DialogTrigger = DialogPrimitive.Trigger;

const DialogPortal = DialogPrimitive.Portal;

const DialogClose = DialogPrimitive.Close;

const DialogOverlay = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Overlay>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Overlay
    ref={ref}
    className={cn(
      "fixed inset-0 z-50 bg-black/80  data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0",
      className,
    )}
    {...props}
  />
));
DialogOverlay.displayName = DialogPrimitive.Overlay.displayName;

const DialogContent = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Content>
>(({ className, children, ...props }, ref) => (
  <DialogPortal>
    <DialogOverlay />
    <DialogPrimitive.Content
      ref={ref}
      className={cn(
        "fixed left-[50%] top-[50%] z-50 grid w-full max-w-lg translate-x-[-50%] translate-y-[-50%] gap-4 border bg-background p-6 shadow-lg duration-200 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[state=closed]:slide-out-to-left-1/2 data-[state=closed]:slide-out-to-top-[48%] data-[state=open]:slide-in-from-left-1/2 data-[state=open]:slide-in-from-top-[48%] sm:rounded-lg",
        className,
      )}
      {...props}
    >
      {children}
      <DialogPrimitive.Close className="absolute right-4 top-4 rounded-sm opacity-70 ring-offset-background transition-opacity hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:pointer-events-none data-[state=open]:bg-accent data-[state=open]:text-muted-foreground">
        <X className="h-4 w-4" />
        <span className="sr-only">Close</span>
      </DialogPrimitive.Close>
    </DialogPrimitive.Content>
  </DialogPortal>
));
DialogContent.displayName = DialogPrimitive.Content.displayName;

const DialogHeader = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    className={cn(
      "flex flex-col space-y-1.5 text-center sm:text-left",
      className,
    )}
    {...props}
  />
);
DialogHeader.displayName = "DialogHeader";

const DialogFooter = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    className={cn(
      "flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-2",
      className,
    )}
    {...props}
  />
);
DialogFooter.displayName = "DialogFooter";

const DialogTitle = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Title>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Title>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Title
    ref={ref}
    className={cn(
      "text-lg font-semibold leading-none tracking-tight",
      className,
    )}
    {...props}
  />
));
DialogTitle.displayName = DialogPrimitive.Title.displayName;

const DialogDescription = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Description>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Description>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Description
    ref={ref}
    className={cn("text-sm text-muted-foreground", className)}
    {...props}
  />
));
DialogDescription.displayName = DialogPrimitive.Description.displayName;

export {
  Dialog,
  DialogPortal,
  DialogOverlay,
  DialogClose,
  DialogTrigger,
  DialogContent,
  DialogHeader,
  DialogFooter,
  DialogTitle,
  DialogDescription,
};

```

### /vercel/path0/examples/music-player/src/components/ui/dropdown-menu.tsx

```ts
import * as DropdownMenuPrimitive from "@radix-ui/react-dropdown-menu";
import { Check, ChevronRight, Circle } from "lucide-react";
import * as React from "react";

import { cn } from "@/lib/utils";

const DropdownMenu = DropdownMenuPrimitive.Root;

const DropdownMenuTrigger = DropdownMenuPrimitive.Trigger;

const DropdownMenuGroup = DropdownMenuPrimitive.Group;

const DropdownMenuPortal = DropdownMenuPrimitive.Portal;

const DropdownMenuSub = DropdownMenuPrimitive.Sub;

const DropdownMenuRadioGroup = DropdownMenuPrimitive.RadioGroup;

const DropdownMenuSubTrigger = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.SubTrigger>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.SubTrigger> & {
    inset?: boolean;
  }
>(({ className, inset, children, ...props }, ref) => (
  <DropdownMenuPrimitive.SubTrigger
    ref={ref}
    className={cn(
      "flex cursor-default select-none items-center rounded-sm px-2 py-1.5 text-sm outline-none focus:bg-accent data-[state=open]:bg-accent",
      inset && "pl-8",
      className,
    )}
    {...props}
  >
    {children}
    <ChevronRight className="ml-auto h-4 w-4" />
  </DropdownMenuPrimitive.SubTrigger>
));
DropdownMenuSubTrigger.displayName =
  DropdownMenuPrimitive.SubTrigger.displayName;

const DropdownMenuSubContent = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.SubContent>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.SubContent>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.SubContent
    ref={ref}
    className={cn(
      "z-50 min-w-[8rem] overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-lg data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2",
      className,
    )}
    {...props}
  />
));
DropdownMenuSubContent.displayName =
  DropdownMenuPrimitive.SubContent.displayName;

const DropdownMenuContent = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Content>
>(({ className, sideOffset = 4, ...props }, ref) => (
  <DropdownMenuPrimitive.Portal>
    <DropdownMenuPrimitive.Content
      ref={ref}
      sideOffset={sideOffset}
      className={cn(
        "z-50 min-w-[8rem] overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2",
        className,
      )}
      {...props}
    />
  </DropdownMenuPrimitive.Portal>
));
DropdownMenuContent.displayName = DropdownMenuPrimitive.Content.displayName;

const DropdownMenuItem = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Item> & {
    inset?: boolean;
  }
>(({ className, inset, ...props }, ref) => (
  <DropdownMenuPrimitive.Item
    ref={ref}
    className={cn(
      "relative flex cursor-default select-none items-center rounded-sm px-2 py-1.5 text-sm outline-none transition-colors focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
      inset && "pl-8",
      className,
    )}
    {...props}
  />
));
DropdownMenuItem.displayName = DropdownMenuPrimitive.Item.displayName;

const DropdownMenuCheckboxItem = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.CheckboxItem>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.CheckboxItem>
>(({ className, children, checked, ...props }, ref) => (
  <DropdownMenuPrimitive.CheckboxItem
    ref={ref}
    className={cn(
      "relative flex cursor-default select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none transition-colors focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
      className,
    )}
    checked={checked}
    {...props}
  >
    <span className="absolute left-2 flex h-3.5 w-3.5 items-center justify-center">
      <DropdownMenuPrimitive.ItemIndicator>
        <Check className="h-4 w-4" />
      </DropdownMenuPrimitive.ItemIndicator>
    </span>
    {children}
  </DropdownMenuPrimitive.CheckboxItem>
));
DropdownMenuCheckboxItem.displayName =
  DropdownMenuPrimitive.CheckboxItem.displayName;

const DropdownMenuRadioItem = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.RadioItem>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.RadioItem>
>(({ className, children, ...props }, ref) => (
  <DropdownMenuPrimitive.RadioItem
    ref={ref}
    className={cn(
      "relative flex cursor-default select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none transition-colors focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
      className,
    )}
    {...props}
  >
    <span className="absolute left-2 flex h-3.5 w-3.5 items-center justify-center">
      <DropdownMenuPrimitive.ItemIndicator>
        <Circle className="h-2 w-2 fill-current" />
      </DropdownMenuPrimitive.ItemIndicator>
    </span>
    {children}
  </DropdownMenuPrimitive.RadioItem>
));
DropdownMenuRadioItem.displayName = DropdownMenuPrimitive.RadioItem.displayName;

const DropdownMenuLabel = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Label>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Label> & {
    inset?: boolean;
  }
>(({ className, inset, ...props }, ref) => (
  <DropdownMenuPrimitive.Label
    ref={ref}
    className={cn(
      "px-2 py-1.5 text-sm font-semibold",
      inset && "pl-8",
      className,
    )}
    {...props}
  />
));
DropdownMenuLabel.displayName = DropdownMenuPrimitive.Label.displayName;

const DropdownMenuSeparator = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Separator>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Separator>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.Separator
    ref={ref}
    className={cn("-mx-1 my-1 h-px bg-muted", className)}
    {...props}
  />
));
DropdownMenuSeparator.displayName = DropdownMenuPrimitive.Separator.displayName;

const DropdownMenuShortcut = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLSpanElement>) => {
  return (
    <span
      className={cn("ml-auto text-xs tracking-widest opacity-60", className)}
      {...props}
    />
  );
};
DropdownMenuShortcut.displayName = "DropdownMenuShortcut";

export {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuCheckboxItem,
  DropdownMenuRadioItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuGroup,
  DropdownMenuPortal,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuRadioGroup,
};

```

### /vercel/path0/examples/music-player/src/components/ui/input.tsx

```ts
import * as React from "react";

import { cn } from "@/lib/utils";

const Input = React.forwardRef<HTMLInputElement, React.ComponentProps<"input">>(
  ({ className, type, ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-base ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 md:text-sm",
          className,
        )}
        ref={ref}
        {...props}
      />
    );
  },
);
Input.displayName = "Input";

export { Input };

```

### /vercel/path0/examples/music-player/src/components/ui/label.tsx

```ts
import * as LabelPrimitive from "@radix-ui/react-label";
import { type VariantProps, cva } from "class-variance-authority";
import * as React from "react";

import { cn } from "@/lib/utils";

const labelVariants = cva(
  "text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70",
);

const Label = React.forwardRef<
  React.ElementRef<typeof LabelPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof LabelPrimitive.Root> &
    VariantProps<typeof labelVariants>
>(({ className, ...props }, ref) => (
  <LabelPrimitive.Root
    ref={ref}
    className={cn(labelVariants(), className)}
    {...props}
  />
));
Label.displayName = LabelPrimitive.Root.displayName;

export { Label };

```

### /vercel/path0/examples/music-player/src/components/ui/separator.tsx

```ts
import * as SeparatorPrimitive from "@radix-ui/react-separator";
import * as React from "react";

import { cn } from "@/lib/utils";

const Separator = React.forwardRef<
  React.ElementRef<typeof SeparatorPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof SeparatorPrimitive.Root>
>(
  (
    { className, orientation = "horizontal", decorative = true, ...props },
    ref,
  ) => (
    <SeparatorPrimitive.Root
      ref={ref}
      decorative={decorative}
      orientation={orientation}
      className={cn(
        "shrink-0 bg-border",
        orientation === "horizontal" ? "h-[1px] w-full" : "h-full w-[1px]",
        className,
      )}
      {...props}
    />
  ),
);
Separator.displayName = SeparatorPrimitive.Root.displayName;

export { Separator };

```

### /vercel/path0/examples/music-player/src/components/ui/sheet.tsx

```ts
"use client";

import * as SheetPrimitive from "@radix-ui/react-dialog";
import { type VariantProps, cva } from "class-variance-authority";
import { X } from "lucide-react";
import * as React from "react";

import { cn } from "@/lib/utils";

const Sheet = SheetPrimitive.Root;

const SheetTrigger = SheetPrimitive.Trigger;

const SheetClose = SheetPrimitive.Close;

const SheetPortal = SheetPrimitive.Portal;

const SheetOverlay = React.forwardRef<
  React.ElementRef<typeof SheetPrimitive.Overlay>,
  React.ComponentPropsWithoutRef<typeof SheetPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <SheetPrimitive.Overlay
    className={cn(
      "fixed inset-0 z-50 bg-black/80  data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0",
      className,
    )}
    {...props}
    ref={ref}
  />
));
SheetOverlay.displayName = SheetPrimitive.Overlay.displayName;

const sheetVariants = cva(
  "fixed z-50 gap-4 bg-background p-6 shadow-lg transition ease-in-out data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:duration-300 data-[state=open]:duration-500",
  {
    variants: {
      side: {
        top: "inset-x-0 top-0 border-b data-[state=closed]:slide-out-to-top data-[state=open]:slide-in-from-top",
        bottom:
          "inset-x-0 bottom-0 border-t data-[state=closed]:slide-out-to-bottom data-[state=open]:slide-in-from-bottom",
        left: "inset-y-0 left-0 h-full w-3/4 border-r data-[state=closed]:slide-out-to-left data-[state=open]:slide-in-from-left sm:max-w-sm",
        right:
          "inset-y-0 right-0 h-full w-3/4  border-l data-[state=closed]:slide-out-to-right data-[state=open]:slide-in-from-right sm:max-w-sm",
      },
    },
    defaultVariants: {
      side: "right",
    },
  },
);

interface SheetContentProps
  extends React.ComponentPropsWithoutRef<typeof SheetPrimitive.Content>,
    VariantProps<typeof sheetVariants> {}

const SheetContent = React.forwardRef<
  React.ElementRef<typeof SheetPrimitive.Content>,
  SheetContentProps
>(({ side = "right", className, children, ...props }, ref) => (
  <SheetPortal>
    <SheetOverlay />
    <SheetPrimitive.Content
      ref={ref}
      className={cn(sheetVariants({ side }), className)}
      {...props}
    >
      {children}
      <SheetPrimitive.Close className="absolute right-4 top-4 rounded-sm opacity-70 ring-offset-background transition-opacity hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:pointer-events-none data-[state=open]:bg-secondary">
        <X className="h-4 w-4" />
        <span className="sr-only">Close</span>
      </SheetPrimitive.Close>
    </SheetPrimitive.Content>
  </SheetPortal>
));
SheetContent.displayName = SheetPrimitive.Content.displayName;

const SheetHeader = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    className={cn(
      "flex flex-col space-y-2 text-center sm:text-left",
      className,
    )}
    {...props}
  />
);
SheetHeader.displayName = "SheetHeader";

const SheetFooter = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    className={cn(
      "flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-2",
      className,
    )}
    {...props}
  />
);
SheetFooter.displayName = "SheetFooter";

const SheetTitle = React.forwardRef<
  React.ElementRef<typeof SheetPrimitive.Title>,
  React.ComponentPropsWithoutRef<typeof SheetPrimitive.Title>
>(({ className, ...props }, ref) => (
  <SheetPrimitive.Title
    ref={ref}
    className={cn("text-lg font-semibold text-foreground", className)}
    {...props}
  />
));
SheetTitle.displayName = SheetPrimitive.Title.displayName;

const SheetDescription = React.forwardRef<
  React.ElementRef<typeof SheetPrimitive.Description>,
  React.ComponentPropsWithoutRef<typeof SheetPrimitive.Description>
>(({ className, ...props }, ref) => (
  <SheetPrimitive.Description
    ref={ref}
    className={cn("text-sm text-muted-foreground", className)}
    {...props}
  />
));
SheetDescription.displayName = SheetPrimitive.Description.displayName;

export {
  Sheet,
  SheetPortal,
  SheetOverlay,
  SheetTrigger,
  SheetClose,
  SheetContent,
  SheetHeader,
  SheetFooter,
  SheetTitle,
  SheetDescription,
};

```

### /vercel/path0/examples/music-player/src/components/ui/sidebar.tsx

```ts
import { Slot } from "@radix-ui/react-slot";
import { VariantProps, cva } from "class-variance-authority";
import { PanelLeft } from "lucide-react";
import * as React from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useIsMobile } from "@/hooks/use-mobile";
import { cn } from "@/lib/utils";

const SIDEBAR_COOKIE_NAME = "sidebar_state";
const SIDEBAR_COOKIE_MAX_AGE = 60 * 60 * 24 * 7;
const SIDEBAR_WIDTH = "16rem";
const SIDEBAR_WIDTH_MOBILE = "18rem";
const SIDEBAR_WIDTH_ICON = "3rem";
const SIDEBAR_KEYBOARD_SHORTCUT = "b";

type SidebarContextProps = {
  state: "expanded" | "collapsed";
  open: boolean;
  setOpen: (open: boolean) => void;
  openMobile: boolean;
  setOpenMobile: (open: boolean) => void;
  isMobile: boolean;
  toggleSidebar: () => void;
};

const SidebarContext = React.createContext<SidebarContextProps | null>(null);

function useSidebar() {
  const context = React.useContext(SidebarContext);
  if (!context) {
    throw new Error("useSidebar must be used within a SidebarProvider.");
  }

  return context;
}

const SidebarProvider = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> & {
    defaultOpen?: boolean;
    open?: boolean;
    onOpenChange?: (open: boolean) => void;
  }
>(
  (
    {
      defaultOpen = true,
      open: openProp,
      onOpenChange: setOpenProp,
      className,
      style,
      children,
      ...props
    },
    ref,
  ) => {
    const isMobile = useIsMobile();
    const [openMobile, setOpenMobile] = React.useState(false);

    // This is the internal state of the sidebar.
    // We use openProp and setOpenProp for control from outside the component.
    const [_open, _setOpen] = React.useState(defaultOpen);
    const open = openProp ?? _open;
    const setOpen = React.useCallback(
      (value: boolean | ((value: boolean) => boolean)) => {
        const openState = typeof value === "function" ? value(open) : value;
        if (setOpenProp) {
          setOpenProp(openState);
        } else {
          _setOpen(openState);
        }

        // This sets the cookie to keep the sidebar state.
        document.cookie = `${SIDEBAR_COOKIE_NAME}=${openState}; path=/; max-age=${SIDEBAR_COOKIE_MAX_AGE}`;
      },
      [setOpenProp, open],
    );

    // Helper to toggle the sidebar.
    const toggleSidebar = React.useCallback(() => {
      return isMobile
        ? setOpenMobile((open) => !open)
        : setOpen((open) => !open);
    }, [isMobile, setOpen, setOpenMobile]);

    // Adds a keyboard shortcut to toggle the sidebar.
    React.useEffect(() => {
      const handleKeyDown = (event: KeyboardEvent) => {
        if (
          event.key === SIDEBAR_KEYBOARD_SHORTCUT &&
          (event.metaKey || event.ctrlKey)
        ) {
          event.preventDefault();
          toggleSidebar();
        }
      };

      window.addEventListener("keydown", handleKeyDown);
      return () => window.removeEventListener("keydown", handleKeyDown);
    }, [toggleSidebar]);

    // We add a state so that we can do data-state="expanded" or "collapsed".
    // This makes it easier to style the sidebar with Tailwind classes.
    const state = open ? "expanded" : "collapsed";

    const contextValue = React.useMemo<SidebarContextProps>(
      () => ({
        state,
        open,
        setOpen,
        isMobile,
        openMobile,
        setOpenMobile,
        toggleSidebar,
      }),
      [
        state,
        open,
        setOpen,
        isMobile,
        openMobile,
        setOpenMobile,
        toggleSidebar,
      ],
    );

    return (
      <SidebarContext.Provider value={contextValue}>
        <TooltipProvider delayDuration={0}>
          <div
            style={
              {
                "--sidebar-width": SIDEBAR_WIDTH,
                "--sidebar-width-icon": SIDEBAR_WIDTH_ICON,
                ...style,
              } as React.CSSProperties
            }
            className={cn(
              "group/sidebar-wrapper flex min-h-svh w-full has-[[data-variant=inset]]:bg-sidebar",
              className,
            )}
            ref={ref}
            {...props}
          >
            {children}
          </div>
        </TooltipProvider>
      </SidebarContext.Provider>
    );
  },
);
SidebarProvider.displayName = "SidebarProvider";

const Sidebar = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> & {
    side?: "left" | "right";
    variant?: "sidebar" | "floating" | "inset";
    collapsible?: "offcanvas" | "icon" | "none";
  }
>(
  (
    {
      side = "left",
      variant = "sidebar",
      collapsible = "offcanvas",
      className,
      children,
      ...props
    },
    ref,
  ) => {
    const { isMobile, state, openMobile, setOpenMobile } = useSidebar();

    if (collapsible === "none") {
      return (
        <div
          className={cn(
            "flex h-full w-[--sidebar-width] flex-col bg-sidebar text-sidebar-foreground",
            className,
          )}
          ref={ref}
          {...props}
        >
          {children}
        </div>
      );
    }

    if (isMobile) {
      return (
        <Sheet open={openMobile} onOpenChange={setOpenMobile} {...props}>
          <SheetContent
            data-sidebar="sidebar"
            data-mobile="true"
            className="w-[--sidebar-width] bg-sidebar p-0 text-sidebar-foreground [&>button]:hidden"
            style={
              {
                "--sidebar-width": SIDEBAR_WIDTH_MOBILE,
              } as React.CSSProperties
            }
            side={side}
          >
            <SheetHeader className="sr-only">
              <SheetTitle>Sidebar</SheetTitle>
              <SheetDescription>Displays the mobile sidebar.</SheetDescription>
            </SheetHeader>
            <div className="flex h-full w-full flex-col">{children}</div>
          </SheetContent>
        </Sheet>
      );
    }

    return (
      <div
        ref={ref}
        className="group peer hidden text-sidebar-foreground md:block"
        data-state={state}
        data-collapsible={state === "collapsed" ? collapsible : ""}
        data-variant={variant}
        data-side={side}
      >
        {/* This is what handles the sidebar gap on desktop */}
        <div
          className={cn(
            "relative w-[--sidebar-width] bg-transparent transition-[width] duration-200 ease-linear",
            "group-data-[collapsible=offcanvas]:w-0",
            "group-data-[side=right]:rotate-180",
            variant === "floating" || variant === "inset"
              ? "group-data-[collapsible=icon]:w-[calc(var(--sidebar-width-icon)_+_theme(spacing.4))]"
              : "group-data-[collapsible=icon]:w-[--sidebar-width-icon]",
          )}
        />
        <div
          className={cn(
            "fixed inset-y-0 z-10 hidden h-svh w-[--sidebar-width] transition-[left,right,width] duration-200 ease-linear md:flex",
            side === "left"
              ? "left-0 group-data-[collapsible=offcanvas]:left-[calc(var(--sidebar-width)*-1)]"
              : "right-0 group-data-[collapsible=offcanvas]:right-[calc(var(--sidebar-width)*-1)]",
            // Adjust the padding for floating and inset variants.
            variant === "floating" || variant === "inset"
              ? "p-2 group-data-[collapsible=icon]:w-[calc(var(--sidebar-width-icon)_+_theme(spacing.4)_+2px)]"
              : "group-data-[collapsible=icon]:w-[--sidebar-width-icon] group-data-[side=left]:border-r group-data-[side=right]:border-l",
            className,
          )}
          {...props}
        >
          <div
            data-sidebar="sidebar"
            className="flex h-full w-full flex-col bg-sidebar group-data-[variant=floating]:rounded-lg group-data-[variant=floating]:border group-data-[variant=floating]:border-sidebar-border group-data-[variant=floating]:shadow"
          >
            {children}
          </div>
        </div>
      </div>
    );
  },
);
Sidebar.displayName = "Sidebar";

const SidebarTrigger = React.forwardRef<
  React.ElementRef<typeof Button>,
  React.ComponentProps<typeof Button>
>(({ className, onClick, ...props }, ref) => {
  const { toggleSidebar } = useSidebar();

  return (
    <Button
      ref={ref}
      data-sidebar="trigger"
      variant="ghost"
      size="icon"
      className={cn("h-7 w-7", className)}
      onClick={(event) => {
        onClick?.(event);
        toggleSidebar();
      }}
      {...props}
    >
      <PanelLeft />
      <span className="sr-only">Toggle Sidebar</span>
    </Button>
  );
});
SidebarTrigger.displayName = "SidebarTrigger";

const SidebarRail = React.forwardRef<
  HTMLButtonElement,
  React.ComponentProps<"button">
>(({ className, ...props }, ref) => {
  const { toggleSidebar } = useSidebar();

  return (
    <button
      ref={ref}
      data-sidebar="rail"
      aria-label="Toggle Sidebar"
      tabIndex={-1}
      onClick={toggleSidebar}
      title="Toggle Sidebar"
      className={cn(
        "absolute inset-y-0 z-20 hidden w-4 -translate-x-1/2 transition-all ease-linear after:absolute after:inset-y-0 after:left-1/2 after:w-[2px] hover:after:bg-sidebar-border group-data-[side=left]:-right-4 group-data-[side=right]:left-0 sm:flex",
        "[[data-side=left]_&]:cursor-w-resize [[data-side=right]_&]:cursor-e-resize",
        "[[data-side=left][data-state=collapsed]_&]:cursor-e-resize [[data-side=right][data-state=collapsed]_&]:cursor-w-resize",
        "group-data-[collapsible=offcanvas]:translate-x-0 group-data-[collapsible=offcanvas]:after:left-full group-data-[collapsible=offcanvas]:hover:bg-sidebar",
        "[[data-side=left][data-collapsible=offcanvas]_&]:-right-2",
        "[[data-side=right][data-collapsible=offcanvas]_&]:-left-2",
        className,
      )}
      {...props}
    />
  );
});
SidebarRail.displayName = "SidebarRail";

const SidebarInset = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"main">
>(({ className, ...props }, ref) => {
  return (
    <main
      ref={ref}
      className={cn(
        "relative flex w-full flex-1 flex-col bg-background",
        "md:peer-data-[variant=inset]:m-2 md:peer-data-[state=collapsed]:peer-data-[variant=inset]:ml-2 md:peer-data-[variant=inset]:ml-0 md:peer-data-[variant=inset]:rounded-xl md:peer-data-[variant=inset]:shadow",
        className,
      )}
      {...props}
    />
  );
});
SidebarInset.displayName = "SidebarInset";

const SidebarInput = React.forwardRef<
  React.ElementRef<typeof Input>,
  React.ComponentProps<typeof Input>
>(({ className, ...props }, ref) => {
  return (
    <Input
      ref={ref}
      data-sidebar="input"
      className={cn(
        "h-8 w-full bg-background shadow-none focus-visible:ring-2 focus-visible:ring-sidebar-ring",
        className,
      )}
      {...props}
    />
  );
});
SidebarInput.displayName = "SidebarInput";

const SidebarHeader = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div">
>(({ className, ...props }, ref) => {
  return (
    <div
      ref={ref}
      data-sidebar="header"
      className={cn("flex flex-col gap-2 p-2", className)}
      {...props}
    />
  );
});
SidebarHeader.displayName = "SidebarHeader";

const SidebarFooter = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div">
>(({ className, ...props }, ref) => {
  return (
    <div
      ref={ref}
      data-sidebar="footer"
      className={cn("flex flex-col gap-2 p-2", className)}
      {...props}
    />
  );
});
SidebarFooter.displayName = "SidebarFooter";

const SidebarSeparator = React.forwardRef<
  React.ElementRef<typeof Separator>,
  React.ComponentProps<typeof Separator>
>(({ className, ...props }, ref) => {
  return (
    <Separator
      ref={ref}
      data-sidebar="separator"
      className={cn("mx-2 w-auto bg-sidebar-border", className)}
      {...props}
    />
  );
});
SidebarSeparator.displayName = "SidebarSeparator";

const SidebarContent = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div">
>(({ className, ...props }, ref) => {
  return (
    <div
      ref={ref}
      data-sidebar="content"
      className={cn(
        "flex min-h-0 flex-1 flex-col gap-2 overflow-auto group-data-[collapsible=icon]:overflow-hidden",
        className,
      )}
      {...props}
    />
  );
});
SidebarContent.displayName = "SidebarContent";

const SidebarGroup = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div">
>(({ className, ...props }, ref) => {
  return (
    <div
      ref={ref}
      data-sidebar="group"
      className={cn("relative flex w-full min-w-0 flex-col p-2", className)}
      {...props}
    />
  );
});
SidebarGroup.displayName = "SidebarGroup";

const SidebarGroupLabel = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> & { asChild?: boolean }
>(({ className, asChild = false, ...props }, ref) => {
  const Comp = asChild ? Slot : "div";

  return (
    <Comp
      ref={ref}
      data-sidebar="group-label"
      className={cn(
        "flex h-8 shrink-0 items-center rounded-md px-2 text-xs font-medium text-sidebar-foreground/70 outline-none ring-sidebar-ring transition-[margin,opacity] duration-200 ease-linear focus-visible:ring-2 [&>svg]:size-4 [&>svg]:shrink-0",
        "group-data-[collapsible=icon]:-mt-8 group-data-[collapsible=icon]:opacity-0",
        className,
      )}
      {...props}
    />
  );
});
SidebarGroupLabel.displayName = "SidebarGroupLabel";

const SidebarGroupAction = React.forwardRef<
  HTMLButtonElement,
  React.ComponentProps<"button"> & { asChild?: boolean }
>(({ className, asChild = false, ...props }, ref) => {
  const Comp = asChild ? Slot : "button";

  return (
    <Comp
      ref={ref}
      data-sidebar="group-action"
      className={cn(
        "absolute right-3 top-3.5 flex aspect-square w-5 items-center justify-center rounded-md p-0 text-sidebar-foreground outline-none ring-sidebar-ring transition-transform hover:bg-sidebar-accent hover:text-sidebar-accent-foreground focus-visible:ring-2 [&>svg]:size-4 [&>svg]:shrink-0",
        // Increases the hit area of the button on mobile.
        "after:absolute after:-inset-2 after:md:hidden",
        "group-data-[collapsible=icon]:hidden",
        className,
      )}
      {...props}
    />
  );
});
SidebarGroupAction.displayName = "SidebarGroupAction";

const SidebarGroupContent = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div">
>(({ className, ...props }, ref) => (
  <div
    ref={ref}
    data-sidebar="group-content"
    className={cn("w-full text-sm", className)}
    {...props}
  />
));
SidebarGroupContent.displayName = "SidebarGroupContent";

const SidebarMenu = React.forwardRef<
  HTMLUListElement,
  React.ComponentProps<"ul">
>(({ className, ...props }, ref) => (
  <ul
    ref={ref}
    data-sidebar="menu"
    className={cn("flex w-full min-w-0 flex-col gap-1", className)}
    {...props}
  />
));
SidebarMenu.displayName = "SidebarMenu";

const SidebarMenuItem = React.forwardRef<
  HTMLLIElement,
  React.ComponentProps<"li">
>(({ className, ...props }, ref) => (
  <li
    ref={ref}
    data-sidebar="menu-item"
    className={cn("group/menu-item relative", className)}
    {...props}
  />
));
SidebarMenuItem.displayName = "SidebarMenuItem";

const sidebarMenuButtonVariants = cva(
  "peer/menu-button flex w-full items-center gap-2 overflow-hidden rounded-md p-2 text-left text-sm outline-none ring-sidebar-ring transition-[width,height,padding] hover:bg-sidebar-accent hover:text-sidebar-accent-foreground focus-visible:ring-2 active:bg-sidebar-accent active:text-sidebar-accent-foreground disabled:pointer-events-none disabled:opacity-50 group-has-[[data-sidebar=menu-action]]/menu-item:pr-8 aria-disabled:pointer-events-none aria-disabled:opacity-50 data-[active=true]:bg-sidebar-accent data-[active=true]:font-medium data-[active=true]:text-sidebar-accent-foreground data-[state=open]:hover:bg-sidebar-accent data-[state=open]:hover:text-sidebar-accent-foreground group-data-[collapsible=icon]:!size-8 group-data-[collapsible=icon]:!p-2 [&>span:last-child]:truncate [&>svg]:size-4 [&>svg]:shrink-0",
  {
    variants: {
      variant: {
        default: "hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
        outline:
          "bg-background shadow-[0_0_0_1px_hsl(var(--sidebar-border))] hover:bg-sidebar-accent hover:text-sidebar-accent-foreground hover:shadow-[0_0_0_1px_hsl(var(--sidebar-accent))]",
      },
      size: {
        default: "h-8 text-sm",
        sm: "h-7 text-xs",
        lg: "h-12 text-sm group-data-[collapsible=icon]:!p-0",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

const SidebarMenuButton = React.forwardRef<
  HTMLButtonElement,
  React.ComponentProps<"button"> & {
    asChild?: boolean;
    isActive?: boolean;
    tooltip?: string | React.ComponentProps<typeof TooltipContent>;
  } & VariantProps<typeof sidebarMenuButtonVariants>
>(
  (
    {
      asChild = false,
      isActive = false,
      variant = "default",
      size = "default",
      tooltip,
      className,
      ...props
    },
    ref,
  ) => {
    const Comp = asChild ? Slot : "button";
    const { isMobile, state } = useSidebar();

    const button = (
      <Comp
        ref={ref}
        data-sidebar="menu-button"
        data-size={size}
        data-active={isActive}
        className={cn(sidebarMenuButtonVariants({ variant, size }), className)}
        {...props}
      />
    );

    if (!tooltip) {
      return button;
    }

    if (typeof tooltip === "string") {
      tooltip = {
        children: tooltip,
      };
    }

    return (
      <Tooltip>
        <TooltipTrigger asChild>{button}</TooltipTrigger>
        <TooltipContent
          side="right"
          align="center"
          hidden={state !== "collapsed" || isMobile}
          {...tooltip}
        />
      </Tooltip>
    );
  },
);
SidebarMenuButton.displayName = "SidebarMenuButton";

const SidebarMenuAction = React.forwardRef<
  HTMLButtonElement,
  React.ComponentProps<"button"> & {
    asChild?: boolean;
    showOnHover?: boolean;
  }
>(({ className, asChild = false, showOnHover = false, ...props }, ref) => {
  const Comp = asChild ? Slot : "button";

  return (
    <Comp
      ref={ref}
      data-sidebar="menu-action"
      className={cn(
        "absolute right-1 top-1.5 flex aspect-square w-5 items-center justify-center rounded-md p-0 text-sidebar-foreground outline-none ring-sidebar-ring transition-transform hover:bg-sidebar-accent hover:text-sidebar-accent-foreground focus-visible:ring-2 peer-hover/menu-button:text-sidebar-accent-foreground [&>svg]:size-4 [&>svg]:shrink-0",
        // Increases the hit area of the button on mobile.
        "after:absolute after:-inset-2 after:md:hidden",
        "peer-data-[size=sm]/menu-button:top-1",
        "peer-data-[size=default]/menu-button:top-1.5",
        "peer-data-[size=lg]/menu-button:top-2.5",
        "group-data-[collapsible=icon]:hidden",
        showOnHover &&
          "group-focus-within/menu-item:opacity-100 group-hover/menu-item:opacity-100 data-[state=open]:opacity-100 peer-data-[active=true]/menu-button:text-sidebar-accent-foreground md:opacity-0",
        className,
      )}
      {...props}
    />
  );
});
SidebarMenuAction.displayName = "SidebarMenuAction";

const SidebarMenuBadge = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div">
>(({ className, ...props }, ref) => (
  <div
    ref={ref}
    data-sidebar="menu-badge"
    className={cn(
      "pointer-events-none absolute right-1 flex h-5 min-w-5 select-none items-center justify-center rounded-md px-1 text-xs font-medium tabular-nums text-sidebar-foreground",
      "peer-hover/menu-button:text-sidebar-accent-foreground peer-data-[active=true]/menu-button:text-sidebar-accent-foreground",
      "peer-data-[size=sm]/menu-button:top-1",
      "peer-data-[size=default]/menu-button:top-1.5",
      "peer-data-[size=lg]/menu-button:top-2.5",
      "group-data-[collapsible=icon]:hidden",
      className,
    )}
    {...props}
  />
));
SidebarMenuBadge.displayName = "SidebarMenuBadge";

const SidebarMenuSkeleton = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> & {
    showIcon?: boolean;
  }
>(({ className, showIcon = false, ...props }, ref) => {
  // Random width between 50 to 90%.
  const width = React.useMemo(() => {
    return `${Math.floor(Math.random() * 40) + 50}%`;
  }, []);

  return (
    <div
      ref={ref}
      data-sidebar="menu-skeleton"
      className={cn("flex h-8 items-center gap-2 rounded-md px-2", className)}
      {...props}
    >
      {showIcon && (
        <Skeleton
          className="size-4 rounded-md"
          data-sidebar="menu-skeleton-icon"
        />
      )}
      <Skeleton
        className="h-4 max-w-[--skeleton-width] flex-1"
        data-sidebar="menu-skeleton-text"
        style={
          {
            "--skeleton-width": width,
          } as React.CSSProperties
        }
      />
    </div>
  );
});
SidebarMenuSkeleton.displayName = "SidebarMenuSkeleton";

const SidebarMenuSub = React.forwardRef<
  HTMLUListElement,
  React.ComponentProps<"ul">
>(({ className, ...props }, ref) => (
  <ul
    ref={ref}
    data-sidebar="menu-sub"
    className={cn(
      "mx-3.5 flex min-w-0 translate-x-px flex-col gap-1 border-l border-sidebar-border px-2.5 py-0.5",
      "group-data-[collapsible=icon]:hidden",
      className,
    )}
    {...props}
  />
));
SidebarMenuSub.displayName = "SidebarMenuSub";

const SidebarMenuSubItem = React.forwardRef<
  HTMLLIElement,
  React.ComponentProps<"li">
>(({ ...props }, ref) => <li ref={ref} {...props} />);
SidebarMenuSubItem.displayName = "SidebarMenuSubItem";

const SidebarMenuSubButton = React.forwardRef<
  HTMLAnchorElement,
  React.ComponentProps<"a"> & {
    asChild?: boolean;
    size?: "sm" | "md";
    isActive?: boolean;
  }
>(({ asChild = false, size = "md", isActive, className, ...props }, ref) => {
  const Comp = asChild ? Slot : "a";

  return (
    <Comp
      ref={ref}
      data-sidebar="menu-sub-button"
      data-size={size}
      data-active={isActive}
      className={cn(
        "flex h-7 min-w-0 -translate-x-px items-center gap-2 overflow-hidden rounded-md px-2 text-sidebar-foreground outline-none ring-sidebar-ring hover:bg-sidebar-accent hover:text-sidebar-accent-foreground focus-visible:ring-2 active:bg-sidebar-accent active:text-sidebar-accent-foreground disabled:pointer-events-none disabled:opacity-50 aria-disabled:pointer-events-none aria-disabled:opacity-50 [&>span:last-child]:truncate [&>svg]:size-4 [&>svg]:shrink-0 [&>svg]:text-sidebar-accent-foreground",
        "data-[active=true]:bg-sidebar-accent data-[active=true]:text-sidebar-accent-foreground",
        size === "sm" && "text-xs",
        size === "md" && "text-sm",
        "group-data-[collapsible=icon]:hidden",
        className,
      )}
      {...props}
    />
  );
});
SidebarMenuSubButton.displayName = "SidebarMenuSubButton";

export {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupAction,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInput,
  SidebarInset,
  SidebarMenu,
  SidebarMenuAction,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSkeleton,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  SidebarProvider,
  SidebarRail,
  SidebarSeparator,
  SidebarTrigger,
  useSidebar,
};

```

### /vercel/path0/examples/music-player/src/components/ui/skeleton.tsx

```ts
import { cn } from "@/lib/utils";

function Skeleton({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("animate-pulse rounded-md bg-muted", className)}
      {...props}
    />
  );
}

export { Skeleton };

```

### /vercel/path0/examples/music-player/src/components/ui/toast.tsx

```ts
import * as ToastPrimitives from "@radix-ui/react-toast";
import { type VariantProps, cva } from "class-variance-authority";
import { X } from "lucide-react";
import * as React from "react";

import { cn } from "@/lib/utils";

const ToastProvider = ToastPrimitives.Provider;

const ToastViewport = React.forwardRef<
  React.ElementRef<typeof ToastPrimitives.Viewport>,
  React.ComponentPropsWithoutRef<typeof ToastPrimitives.Viewport>
>(({ className, ...props }, ref) => (
  <ToastPrimitives.Viewport
    ref={ref}
    className={cn(
      "fixed top-0 z-[100] flex max-h-screen w-full flex-col-reverse p-4 sm:bottom-0 sm:right-0 sm:top-auto sm:flex-col md:max-w-[420px]",
      className,
    )}
    {...props}
  />
));
ToastViewport.displayName = ToastPrimitives.Viewport.displayName;

const toastVariants = cva(
  "group pointer-events-auto relative flex w-full items-center justify-between space-x-4 overflow-hidden rounded-md border p-6 pr-8 shadow-lg transition-all data-[swipe=cancel]:translate-x-0 data-[swipe=end]:translate-x-[var(--radix-toast-swipe-end-x)] data-[swipe=move]:translate-x-[var(--radix-toast-swipe-move-x)] data-[swipe=move]:transition-none data-[state=open]:animate-in data-[state=closed]:animate-out data-[swipe=end]:animate-out data-[state=closed]:fade-out-80 data-[state=closed]:slide-out-to-right-full data-[state=open]:slide-in-from-top-full data-[state=open]:sm:slide-in-from-bottom-full",
  {
    variants: {
      variant: {
        default: "border bg-background text-foreground",
        destructive:
          "destructive group border-destructive bg-destructive text-destructive-foreground",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

const Toast = React.forwardRef<
  React.ElementRef<typeof ToastPrimitives.Root>,
  React.ComponentPropsWithoutRef<typeof ToastPrimitives.Root> &
    VariantProps<typeof toastVariants>
>(({ className, variant, ...props }, ref) => {
  return (
    <ToastPrimitives.Root
      ref={ref}
      className={cn(toastVariants({ variant }), className)}
      {...props}
    />
  );
});
Toast.displayName = ToastPrimitives.Root.displayName;

const ToastAction = React.forwardRef<
  React.ElementRef<typeof ToastPrimitives.Action>,
  React.ComponentPropsWithoutRef<typeof ToastPrimitives.Action>
>(({ className, ...props }, ref) => (
  <ToastPrimitives.Action
    ref={ref}
    className={cn(
      "inline-flex h-8 shrink-0 items-center justify-center rounded-md border bg-transparent px-3 text-sm font-medium ring-offset-background transition-colors hover:bg-secondary focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 group-[.destructive]:border-muted/40 group-[.destructive]:hover:border-destructive/30 group-[.destructive]:hover:bg-destructive group-[.destructive]:hover:text-destructive-foreground group-[.destructive]:focus:ring-destructive",
      className,
    )}
    {...props}
  />
));
ToastAction.displayName = ToastPrimitives.Action.displayName;

const ToastClose = React.forwardRef<
  React.ElementRef<typeof ToastPrimitives.Close>,
  React.ComponentPropsWithoutRef<typeof ToastPrimitives.Close>
>(({ className, ...props }, ref) => (
  <ToastPrimitives.Close
    ref={ref}
    className={cn(
      "absolute right-2 top-2 rounded-md p-1 text-foreground/50 opacity-0 transition-opacity hover:text-foreground focus:opacity-100 focus:outline-none focus:ring-2 group-hover:opacity-100 group-[.destructive]:text-red-300 group-[.destructive]:hover:text-red-50 group-[.destructive]:focus:ring-red-400 group-[.destructive]:focus:ring-offset-red-600",
      className,
    )}
    toast-close=""
    {...props}
  >
    <X className="h-4 w-4" />
  </ToastPrimitives.Close>
));
ToastClose.displayName = ToastPrimitives.Close.displayName;

const ToastTitle = React.forwardRef<
  React.ElementRef<typeof ToastPrimitives.Title>,
  React.ComponentPropsWithoutRef<typeof ToastPrimitives.Title>
>(({ className, ...props }, ref) => (
  <ToastPrimitives.Title
    ref={ref}
    className={cn("text-sm font-semibold", className)}
    {...props}
  />
));
ToastTitle.displayName = ToastPrimitives.Title.displayName;

const ToastDescription = React.forwardRef<
  React.ElementRef<typeof ToastPrimitives.Description>,
  React.ComponentPropsWithoutRef<typeof ToastPrimitives.Description>
>(({ className, ...props }, ref) => (
  <ToastPrimitives.Description
    ref={ref}
    className={cn("text-sm opacity-90", className)}
    {...props}
  />
));
ToastDescription.displayName = ToastPrimitives.Description.displayName;

type ToastProps = React.ComponentPropsWithoutRef<typeof Toast>;

type ToastActionElement = React.ReactElement<typeof ToastAction>;

export {
  type ToastProps,
  type ToastActionElement,
  ToastProvider,
  ToastViewport,
  Toast,
  ToastTitle,
  ToastDescription,
  ToastClose,
  ToastAction,
};

```

### /vercel/path0/examples/music-player/src/components/ui/toaster.tsx

```ts
import {
  Toast,
  ToastClose,
  ToastDescription,
  ToastProvider,
  ToastTitle,
  ToastViewport,
} from "@/components/ui/toast";
import { useToast } from "@/hooks/use-toast";

export function Toaster() {
  const { toasts } = useToast();

  return (
    <ToastProvider>
      {toasts.map(function ({ id, title, description, action, ...props }) {
        return (
          <Toast key={id} {...props}>
            <div className="grid gap-1">
              {title && <ToastTitle>{title}</ToastTitle>}
              {description && (
                <ToastDescription>{description}</ToastDescription>
              )}
            </div>
            {action}
            <ToastClose />
          </Toast>
        );
      })}
      <ToastViewport />
    </ToastProvider>
  );
}

```

### /vercel/path0/examples/music-player/src/components/ui/tooltip.tsx

```ts
import * as TooltipPrimitive from "@radix-ui/react-tooltip";
import * as React from "react";

import { cn } from "@/lib/utils";

const TooltipProvider = TooltipPrimitive.Provider;

const Tooltip = TooltipPrimitive.Root;

const TooltipTrigger = TooltipPrimitive.Trigger;

const TooltipContent = React.forwardRef<
  React.ElementRef<typeof TooltipPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TooltipPrimitive.Content>
>(({ className, sideOffset = 4, ...props }, ref) => (
  <TooltipPrimitive.Content
    ref={ref}
    sideOffset={sideOffset}
    className={cn(
      "z-50 overflow-hidden rounded-md border bg-popover px-3 py-1.5 text-sm text-popover-foreground shadow-md animate-in fade-in-0 zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2 origin-[--radix-tooltip-content-transform-origin]",
      className,
    )}
    {...props}
  />
));
TooltipContent.displayName = TooltipPrimitive.Content.displayName;

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider };

```

### /vercel/path0/examples/music-player/src/hooks/use-mobile.tsx

```ts
import * as React from "react";

const MOBILE_BREAKPOINT = 768;

export function useIsMobile() {
  const [isMobile, setIsMobile] = React.useState<boolean | undefined>(
    undefined,
  );

  React.useEffect(() => {
    const mql = window.matchMedia(`(max-width: ${MOBILE_BREAKPOINT - 1}px)`);
    const onChange = () => {
      setIsMobile(window.innerWidth < MOBILE_BREAKPOINT);
    };
    mql.addEventListener("change", onChange);
    setIsMobile(window.innerWidth < MOBILE_BREAKPOINT);
    return () => mql.removeEventListener("change", onChange);
  }, []);

  return !!isMobile;
}

```

### /vercel/path0/examples/music-player/src/hooks/use-toast.ts

```ts
import * as React from "react";

import type { ToastActionElement, ToastProps } from "@/components/ui/toast";

const TOAST_LIMIT = 1;
const TOAST_REMOVE_DELAY = 1000000;

type ToasterToast = ToastProps & {
  id: string;
  title?: React.ReactNode;
  description?: React.ReactNode;
  action?: ToastActionElement;
};

const actionTypes = {
  ADD_TOAST: "ADD_TOAST",
  UPDATE_TOAST: "UPDATE_TOAST",
  DISMISS_TOAST: "DISMISS_TOAST",
  REMOVE_TOAST: "REMOVE_TOAST",
} as const;

let count = 0;

function genId() {
  count = (count + 1) % Number.MAX_SAFE_INTEGER;
  return count.toString();
}

type ActionType = typeof actionTypes;

type Action =
  | {
      type: ActionType["ADD_TOAST"];
      toast: ToasterToast;
    }
  | {
      type: ActionType["UPDATE_TOAST"];
      toast: Partial<ToasterToast>;
    }
  | {
      type: ActionType["DISMISS_TOAST"];
      toastId?: ToasterToast["id"];
    }
  | {
      type: ActionType["REMOVE_TOAST"];
      toastId?: ToasterToast["id"];
    };

interface State {
  toasts: ToasterToast[];
}

const toastTimeouts = new Map<string, ReturnType<typeof setTimeout>>();

const addToRemoveQueue = (toastId: string) => {
  if (toastTimeouts.has(toastId)) {
    return;
  }

  const timeout = setTimeout(() => {
    toastTimeouts.delete(toastId);
    dispatch({
      type: "REMOVE_TOAST",
      toastId: toastId,
    });
  }, TOAST_REMOVE_DELAY);

  toastTimeouts.set(toastId, timeout);
};

export const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case "ADD_TOAST":
      return {
        ...state,
        toasts: [action.toast, ...state.toasts].slice(0, TOAST_LIMIT),
      };

    case "UPDATE_TOAST":
      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === action.toast.id ? { ...t, ...action.toast } : t,
        ),
      };

    case "DISMISS_TOAST": {
      const { toastId } = action;

      // ! Side effects ! - This could be extracted into a dismissToast() action,
      // but I'll keep it here for simplicity
      if (toastId) {
        addToRemoveQueue(toastId);
      } else {
        state.toasts.forEach((toast) => {
          addToRemoveQueue(toast.id);
        });
      }

      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === toastId || toastId === undefined
            ? {
                ...t,
                open: false,
              }
            : t,
        ),
      };
    }
    case "REMOVE_TOAST":
      if (action.toastId === undefined) {
        return {
          ...state,
          toasts: [],
        };
      }
      return {
        ...state,
        toasts: state.toasts.filter((t) => t.id !== action.toastId),
      };
  }
};

const listeners: Array<(state: State) => void> = [];

let memoryState: State = { toasts: [] };

function dispatch(action: Action) {
  memoryState = reducer(memoryState, action);
  listeners.forEach((listener) => {
    listener(memoryState);
  });
}

type Toast = Omit<ToasterToast, "id">;

function toast({ ...props }: Toast) {
  const id = genId();

  const update = (props: ToasterToast) =>
    dispatch({
      type: "UPDATE_TOAST",
      toast: { ...props, id },
    });
  const dismiss = () => dispatch({ type: "DISMISS_TOAST", toastId: id });

  dispatch({
    type: "ADD_TOAST",
    toast: {
      ...props,
      id,
      open: true,
      onOpenChange: (open) => {
        if (!open) dismiss();
      },
    },
  });

  return {
    id: id,
    dismiss,
    update,
  };
}

function useToast() {
  const [state, setState] = React.useState<State>(memoryState);

  React.useEffect(() => {
    listeners.push(setState);
    return () => {
      const index = listeners.indexOf(setState);
      if (index > -1) {
        listeners.splice(index, 1);
      }
    };
  }, [state]);

  return {
    ...state,
    toast,
    dismiss: (toastId?: string) => dispatch({ type: "DISMISS_TOAST", toastId }),
  };
}

export { useToast, toast };

```

### /vercel/path0/examples/music-player/src/index.css

```ts
@tailwind base;
@tailwind components;
@tailwind utilities;

@layer base {
  :root {
    --background: 0 0% 100%;
    --foreground: 20 14.3% 4.1%;

    --card: 0 0% 100%;
    --card-foreground: 20 14.3% 4.1%;

    --popover: 0 0% 100%;
    --popover-foreground: 20 14.3% 4.1%;

    --primary: 24 9.8% 10%;
    --primary-foreground: 60 9.1% 97.8%;

    --secondary: 60 4.8% 95.9%;
    --secondary-foreground: 24 9.8% 10%;

    --muted: 60 4.8% 95.9%;
    --muted-foreground: 25 5.3% 44.7%;

    --accent: 60 4.8% 95.9%;
    --accent-foreground: 24 9.8% 10%;

    --destructive: 0 84.2% 60.2%;
    --destructive-foreground: 60 9.1% 97.8%;

    --border: 20 5.9% 90%;
    --input: 20 5.9% 90%;
    --ring: 20 14.3% 4.1%;

    --radius: 0.5rem;

    --sidebar-background: 0 0% 98%;

    --sidebar-foreground: 240 5.3% 26.1%;

    --sidebar-primary: 240 5.9% 10%;

    --sidebar-primary-foreground: 0 0% 98%;

    --sidebar-accent: 240 4.8% 95.9%;

    --sidebar-accent-foreground: 240 5.9% 10%;

    --sidebar-border: 220 13% 91%;

    --sidebar-ring: 217.2 91.2% 59.8%;
  }

  .dark {
    --background: 20 14.3% 4.1%;
    --foreground: 60 9.1% 97.8%;

    --card: 20 14.3% 4.1%;
    --card-foreground: 60 9.1% 97.8%;

    --popover: 20 14.3% 4.1%;
    --popover-foreground: 60 9.1% 97.8%;

    --primary: 60 9.1% 97.8%;
    --primary-foreground: 24 9.8% 10%;

    --secondary: 12 6.5% 15.1%;
    --secondary-foreground: 60 9.1% 97.8%;

    --muted: 12 6.5% 15.1%;
    --muted-foreground: 24 5.4% 63.9%;

    --accent: 12 6.5% 15.1%;
    --accent-foreground: 60 9.1% 97.8%;

    --destructive: 0 62.8% 30.6%;
    --destructive-foreground: 60 9.1% 97.8%;

    --border: 12 6.5% 15.1%;
    --input: 12 6.5% 15.1%;
    --ring: 24 5.7% 82.9%;
    --sidebar-background: 240 5.9% 10%;
    --sidebar-foreground: 240 4.8% 95.9%;
    --sidebar-primary: 224.3 76.3% 48%;
    --sidebar-primary-foreground: 0 0% 100%;
    --sidebar-accent: 240 3.7% 15.9%;
    --sidebar-accent-foreground: 240 4.8% 95.9%;
    --sidebar-border: 240 3.7% 15.9%;
    --sidebar-ring: 217.2 91.2% 59.8%;
  }
}

@layer base {
  * {
    @apply border-border;
  }
  body {
    @apply bg-background text-foreground;
  }
}

```

### /vercel/path0/examples/music-player/src/lib/audio/AudioManager.ts

```ts
import { createContext, useContext } from "react";

export class AudioManager {
  mediaElement: HTMLAudioElement;

  audioObjectURL: string | null = null;

  constructor() {
    const mediaElement = new Audio();

    this.mediaElement = mediaElement;
  }

  async unloadCurrentAudio() {
    if (this.audioObjectURL) {
      URL.revokeObjectURL(this.audioObjectURL);
      this.audioObjectURL = null;
    }
  }

  async loadAudio(file: Blob) {
    await this.unloadCurrentAudio();

    const { mediaElement } = this;
    const audioObjectURL = URL.createObjectURL(file);

    this.audioObjectURL = audioObjectURL;

    mediaElement.src = audioObjectURL;
  }

  play() {
    if (this.mediaElement.ended) {
      this.mediaElement.fastSeek(0);
    }

    this.mediaElement.play();
  }

  pause() {
    this.mediaElement.pause();
  }

  destroy() {
    this.unloadCurrentAudio();
    this.mediaElement.pause();
  }
}

const context = createContext<AudioManager>(new AudioManager());

export function useAudioManager() {
  return useContext(context);
}

export const AudionManagerProvider = context.Provider;

```

### /vercel/path0/examples/music-player/src/lib/audio/getAudioFileData.ts

```ts
export async function getAudioFileData(file: Blob, samples = 200) {
  const ctx = new AudioContext();

  const buffer = await file.arrayBuffer();
  const decodedAudio = await ctx.decodeAudioData(buffer);

  return {
    waveform: transformDecodedAudioToWaveformData(decodedAudio, samples),
    duration: decodedAudio.duration,
  };
}

const transformDecodedAudioToWaveformData = (
  audioBuffer: AudioBuffer,
  samples: number,
) => {
  const rawData = audioBuffer.getChannelData(0); // We only need to work with one channel of data
  const blockSize = Math.floor(rawData.length / samples); // the number of samples in each subdivision

  const sampledData: number[] = new Array(samples);
  let max = 0;

  for (let i = 0; i < samples; i++) {
    const blockStart = blockSize * i; // the location of the first sample in the block
    let sum = 0;
    for (let j = 0; j < blockSize; j++) {
      sum = sum + Math.abs(rawData[blockStart + j]); // find the sum of all the samples in the block
    }
    const sampledValue = sum / blockSize; // divide the sum by the block size to get the average

    if (max < sampledValue) {
      max = sampledValue;
    }

    sampledData[i] = sampledValue;
  }

  const multiplier = max ** -1;

  for (let i = 0; i < samples; i++) {
    sampledData[i] = sampledData[i] * multiplier;
  }

  return sampledData;
};

```

### /vercel/path0/examples/music-player/src/lib/audio/useMediaEndListener.ts

```ts
import { useEffect } from "react";
import { useAudioManager } from "./AudioManager";

export function useMediaEndListener(callback: () => void) {
  const audioManager = useAudioManager();

  useEffect(() => {
    audioManager.mediaElement.addEventListener("ended", callback);

    return () => {
      audioManager.mediaElement.removeEventListener("ended", callback);
    };
  }, [audioManager, callback]);
}

```

### /vercel/path0/examples/music-player/src/lib/audio/usePlayMedia.ts

```ts
import { useRef } from "react";
import { useAudioManager } from "./AudioManager";

export function usePlayMedia() {
  const audioManager = useAudioManager();

  const previousMediaLoad = useRef<Promise<unknown> | undefined>(undefined);

  async function playMedia(file: Blob) {
    // Wait for the previous load to finish
    // to avoid to incur into concurrency issues
    await previousMediaLoad.current;

    const promise = audioManager.loadAudio(file);

    previousMediaLoad.current = promise;

    await promise;

    audioManager.play();
  }

  return playMedia;
}

```

### /vercel/path0/examples/music-player/src/lib/audio/usePlayState.ts

```ts
import { useLayoutEffect, useState } from "react";
import { useAudioManager } from "./AudioManager";

export type PlayState = "pause" | "play";

export function usePlayState() {
  const audioManager = useAudioManager();
  const [value, setValue] = useState<PlayState>("pause");

  useLayoutEffect(() => {
    setValue(audioManager.mediaElement.paused ? "pause" : "play");

    const onPlay = () => {
      setValue("play");
    };

    const onPause = () => {
      setValue("pause");
    };

    audioManager.mediaElement.addEventListener("play", onPlay);
    audioManager.mediaElement.addEventListener("pause", onPause);

    return () => {
      audioManager.mediaElement.removeEventListener("play", onPlay);
      audioManager.mediaElement.removeEventListener("pause", onPause);
    };
  }, [audioManager]);

  function togglePlayState() {
    if (value === "pause") {
      audioManager.play();
    } else {
      audioManager.pause();
    }
  }

  return { value, toggle: togglePlayState };
}

```

### /vercel/path0/examples/music-player/src/lib/audio/usePlayerCurrentTime.ts

```ts
import { useLayoutEffect, useState } from "react";
import { useAudioManager } from "./AudioManager";

export function usePlayerCurrentTime() {
  const audioManager = useAudioManager();
  const [value, setValue] = useState<number>(0);

  useLayoutEffect(() => {
    setValue(audioManager.mediaElement.currentTime);

    const onTimeUpdate = () => {
      setValue(audioManager.mediaElement.currentTime);
    };

    audioManager.mediaElement.addEventListener("timeupdate", onTimeUpdate);

    return () => {
      audioManager.mediaElement.removeEventListener("timeupdate", onTimeUpdate);
    };
  }, [audioManager]);

  function setCurrentTime(time: number) {
    if (audioManager.mediaElement.paused) audioManager.play();

    // eslint-disable-next-line react-compiler/react-compiler
    audioManager.mediaElement.currentTime = time;
  }

  return {
    value,
    setValue: setCurrentTime,
  };
}

```

### /vercel/path0/examples/music-player/src/lib/getters.ts

```ts
import { MusicaAccount } from "../1_schema";

export async function getNextTrack() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        activePlaylist: {
          tracks: true,
        },
      },
    },
  });

  const tracks = me.root.activePlaylist.tracks;
  const activeTrack = me.root._refs.activeTrack;

  const currentIndex = tracks.findIndex((item) => item?.id === activeTrack?.id);

  const nextIndex = (currentIndex + 1) % tracks.length;

  return tracks[nextIndex];
}

export async function getPrevTrack() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        activePlaylist: {
          tracks: true,
        },
      },
    },
  });

  const tracks = me.root.activePlaylist.tracks;
  const activeTrack = me.root._refs.activeTrack;

  const currentIndex = tracks.findIndex((item) => item?.id === activeTrack?.id);

  const previousIndex = (currentIndex - 1 + tracks.length) % tracks.length;
  return tracks[previousIndex];
}

```

### /vercel/path0/examples/music-player/src/lib/useKeyboardListener.ts

```ts
import { useEffect } from "react";

export function useKeyboardListener(code: string, callback: () => void) {
  useEffect(() => {
    const handler = (evt: KeyboardEvent) => {
      if (evt.code === code) {
        callback();
      }
    };
    window.addEventListener("keyup", handler);

    return () => {
      window.removeEventListener("keyup", handler);
    };
  }, [callback, code]);
}

```

### /vercel/path0/examples/music-player/src/lib/useUploadExampleData.ts

```ts
import { MusicaAccount } from "@/1_schema";
import { useAccount } from "jazz-react";
import { useEffect } from "react";
import { uploadMusicTracks } from "../4_actions";

export function useUploadExampleData() {
  const { me } = useAccount();

  useEffect(() => {
    uploadOnboardingData();
  }, [me.id]);
}

async function uploadOnboardingData() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: { root: true },
  });

  if (me.root.exampleDataLoaded) return;

  me.root.exampleDataLoaded = true;

  try {
    const trackFile = await (await fetch("/example.mp3")).blob();

    await uploadMusicTracks([new File([trackFile], "Example song")], true);
  } catch (error) {
    me.root.exampleDataLoaded = false;
    throw error;
  }
}

```

### /vercel/path0/examples/music-player/src/lib/utils.ts

```ts
import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

```

### /vercel/path0/examples/music-player/src/vite-env.d.ts

```ts
/// <reference types="vite/client" />

```
