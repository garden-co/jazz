import { co, z, CoMap, Group, type Db } from "jazz-tools";
// `../../` from here and from the scratch copy scripts/classic-api.test.mjs
// compiles (packages/jazz-tools/.test-tmp/<dir>/) both reach the package root.
import { app, type Todo } from "../../tests/public-api/apps/todos/jazz.generated.js";
import {
  useCoState,
  useAccount,
  useSuspenseCoState,
  useSuspenseAccount,
  JazzReactProvider,
  JazzProvider,
  useAll,
} from "jazz-tools/react";
import { useCoState as coreCoState } from "jazz-tools/react-core";
import { useCoState as nativeCoState, JazzReactNativeProvider } from "jazz-tools/react-native";
import { useCoState as expoCoState, JazzExpoProvider } from "jazz-tools/expo";
import {
  useCoState as vueCoState,
  useAccount as vueAccount,
  useAccountOrGuest,
  useJazzContext,
  useAcceptInvite,
  type JazzProviderProps as VueProviderProps,
} from "jazz-tools/vue";
import type { JazzProviderProps as SolidProviderProps } from "jazz-tools/solid";

// These imports must resolve against emitted declarations, never tsconfig src aliases.
// @ts-expect-error Classic members must not become valid schema builders.
co.map({});
// @ts-expect-error The old package's Zod reexport is not a Jazz 2 schema builder.
z.string();
// @ts-expect-error Classic static operations remain invalid.
Group.create();
// @ts-expect-error Classic values cannot be called.
CoMap();
// @ts-expect-error Classic values cannot be constructed.
new CoMap();
// @ts-expect-error Classic values cannot be subclassed.
class OldMap extends CoMap {}
// @ts-expect-error Classic hooks are not callable, even without arguments.
useCoState();
// @ts-expect-error Classic account loading is not a supported hook.
useAccount();
// @ts-expect-error Classic Suspense hooks are not callable.
useSuspenseCoState();
// @ts-expect-error Classic account Suspense hooks are not callable.
useSuspenseAccount();
// @ts-expect-error Shared React reexports must preserve rejection.
coreCoState();
// @ts-expect-error Native reexports must preserve rejection.
nativeCoState();
// @ts-expect-error Expo reexports must preserve rejection.
expoCoState();
// @ts-expect-error Classic providers are not valid JSX components.
const oldReact = <JazzReactProvider />;
// @ts-expect-error Classic native providers are not valid JSX components.
const oldNative = <JazzReactNativeProvider />;
// @ts-expect-error Classic Expo providers are not valid JSX components.
const oldExpo = <JazzExpoProvider />;

// @ts-expect-error Vue Classic composables are not callable.
vueCoState();
// @ts-expect-error Vue Classic account loading remains invalid.
vueAccount();
// @ts-expect-error Vue Classic guest account loading remains invalid.
useAccountOrGuest();
// @ts-expect-error Vue Classic context is not the Jazz 2 client context.
useJazzContext();
// @ts-expect-error Vue Classic invites remain invalid.
useAcceptInvite();

const oldVueProps: VueProviderProps = {
  config: { scope: "classic-vue-types" },
  // @ts-expect-error Diagnostics must not make Classic props supported.
  AccountSchema: undefined,
};
const currentVueProps: VueProviderProps = { config: { scope: "current-vue-types" } };
const currentSolidProps: SolidProviderProps = {
  config: { scope: "current-solid-types" },
  children: "Jazz 2",
};
void [oldVueProps, currentVueProps, currentSolidProps];

// Positive consumers prevent unrelated module/type-resolution failures from hiding
// accidental rejection of the supported Jazz 2 schema, query and React interfaces.
declare const db: Db;
db.insert(app.todos, { title: "Jazz 2", done: false });
const todo: Todo = { id: "todo", title: "Jazz 2", done: false };
function Todos() {
  const { data } = useAll(app.todos.where({ done: false }));
  return data?.map((row) => <p key={row.id}>{row.title}</p>);
}
const currentReact = (
  <JazzProvider config={{ scope: "classic-diagnostic-types" }}>
    <Todos />
  </JazzProvider>
);
void [todo, currentReact, oldReact, oldNative, oldExpo, OldMap];
