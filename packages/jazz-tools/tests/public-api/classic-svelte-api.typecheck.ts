import type { ComponentProps } from "svelte";
import {
  CoState,
  AccountCoState,
  InviteListener,
  SyncConnectionStatus,
  JazzSvelteProvider,
} from "jazz-tools/svelte";

// Svelte consumers use Bundler resolution to read generated .svelte.d.ts files.
// NodeNext can fall back to Svelte's ambient, permissive *.svelte declaration.
// @ts-expect-error Classic Svelte classes cannot be constructed.
new CoState();
// @ts-expect-error Classic account subscriptions remain invalid.
new AccountCoState();
// @ts-expect-error Classic invite listeners remain invalid.
new InviteListener();
// @ts-expect-error Classic sync state remains invalid.
new SyncConnectionStatus();

declare const children: ComponentProps<typeof JazzSvelteProvider>["children"];
const oldProps: ComponentProps<typeof JazzSvelteProvider> = {
  config: { appId: "classic-svelte-types" },
  children,
  // @ts-expect-error Diagnostics must not make Classic props supported.
  sync: undefined,
};
const currentProps: ComponentProps<typeof JazzSvelteProvider> = {
  config: { appId: "current-svelte-types" },
  children,
};
void [oldProps, currentProps];
