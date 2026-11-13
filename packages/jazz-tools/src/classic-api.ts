declare const classicApiRemoved: unique symbol;

/** An unsupported Jazz Classic value, deliberately neither callable nor constructable. */
interface JazzClassicApiRemoved {
  readonly [classicApiRemoved]: never;
}

const TABLES =
  'tables: `import { schema as s } from "jazz-tools"`, `s.table({ ... })` in `s.defineApp({ ... })`';
const QUERY_HOOKS = "`useAll(query)` or `useOne(query)` (Svelte: `QuerySubscription`)";
const SESSION =
  "the session: `useJazzSession()` (Svelte: `getJazzSession()`), whose `user` is who is signed in";
const PROVIDER = '`<JazzProvider config={{ serverUrl: "/jazz" }}>`';

/**
 * The Jazz 2 replacement for a Classic name, where there is one. Not a rename:
 * the data model differs (see the README), but it points at where to start.
 */
const CLASSIC_REPLACEMENTS: Record<string, string> = {
  co: TABLES,
  z: TABLES,
  CoMap: TABLES,
  CoList: TABLES,
  Group: "row-level permission policies: `s.definePermissions(app, ...)`",
  Account: SESSION,
  useCoState: QUERY_HOOKS,
  useSuspenseCoState: "`useAllSuspense(query)` or `useOneSuspense(query)`",
  CoState: "`QuerySubscription` / `QuerySubscriptionOne`",
  useAccount: SESSION,
  useSuspenseAccount: SESSION,
  useAccountOrGuest: SESSION,
  AccountCoState: SESSION,
  useJazzContext: "`useDb()` for the database, and " + SESSION,
  JazzReactProvider: PROVIDER,
  JazzReactNativeProvider: PROVIDER,
  JazzExpoProvider: PROVIDER,
  SyncConnectionStatus: "`syncStatus()`",
  sync: '`config={{ serverUrl: "/jazz" }}`',
};

function failClassicApi(operation: string): never {
  // `co.map` and `JazzProvider.sync` are named by their Classic part.
  const [head, member] = operation.split(".");
  const known = (name: string | undefined) =>
    name !== undefined && Object.hasOwn(CLASSIC_REPLACEMENTS, name)
      ? CLASSIC_REPLACEMENTS[name]
      : undefined;
  const replacement = known(head) ?? known(member);
  const error = new Error(
    `[JAZZ_CLASSIC_API_REMOVED] ${operation} belongs to Jazz Classic (0.x) ` +
      `and is not supported by this Jazz 2 package.\n\n` +
      (replacement ? `In Jazz 2, start from ${replacement}.\n\n` : "") +
      `Jazz 2 uses relational tables, queries, and row-level permission policies. ` +
      `This is not a rename-only migration.\n\n` +
      `Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt before continuing.`,
  );
  throw Object.assign(error, { code: "JAZZ_CLASSIC_API_REMOVED" });
}

const classicProviderProps = ["sync", "AccountSchema", "accountSchema", "account-schema"] as const;

/** @internal Reused provider names must reject Classic configuration, not valid Jazz 2 usage. */
export function assertNoClassicProviderProps(provider: string, props: object): void {
  for (const property of classicProviderProps) {
    if (Object.hasOwn(props, property)) {
      failClassicApi(`${provider}.${property}`);
    }
  }
}

function classicFunction(name: string) {
  // Keep ordinary function metadata: React may inspect it before rendering.
  return function removedClassicApi(): never {
    return failClassicApi(name);
  };
}

function classicNamespace(name: string): JazzClassicApiRemoved {
  // Framework export inspection must be inert, including Fast Refresh.
  // Classic calls/construction and domain-member reads still fail.
  return new Proxy(classicFunction(name), {
    get(target, property) {
      if (property === "prototype" || property === "name" || property === "length") {
        return Reflect.get(target, property);
      }
      return failClassicApi(`${name}.${String(property)}`);
    },
  }) as unknown as JazzClassicApiRemoved;
}

/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const co: JazzClassicApiRemoved = classicNamespace("co");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const z: JazzClassicApiRemoved = classicNamespace("z");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const CoMap: JazzClassicApiRemoved = classicNamespace("CoMap");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const CoList: JazzClassicApiRemoved = classicNamespace("CoList");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const CoFeed: JazzClassicApiRemoved = classicNamespace("CoFeed");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const CoPlainText: JazzClassicApiRemoved = classicNamespace("CoPlainText");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const CoRichText: JazzClassicApiRemoved = classicNamespace("CoRichText");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const FileStream: JazzClassicApiRemoved = classicNamespace("FileStream");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const Account: JazzClassicApiRemoved = classicNamespace("Account");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const Group: JazzClassicApiRemoved = classicNamespace("Group");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const Profile: JazzClassicApiRemoved = classicNamespace("Profile");

/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const useCoState: JazzClassicApiRemoved = classicFunction(
  "useCoState",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const useAccount: JazzClassicApiRemoved = classicFunction(
  "useAccount",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const useSuspenseCoState: JazzClassicApiRemoved = classicFunction(
  "useSuspenseCoState",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const useSuspenseAccount: JazzClassicApiRemoved = classicFunction(
  "useSuspenseAccount",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const JazzReactProvider: JazzClassicApiRemoved = classicFunction(
  "JazzReactProvider",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const JazzReactNativeProvider: JazzClassicApiRemoved = classicFunction(
  "JazzReactNativeProvider",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const JazzExpoProvider: JazzClassicApiRemoved = classicFunction(
  "JazzExpoProvider",
) as unknown as JazzClassicApiRemoved;

/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const CoState: JazzClassicApiRemoved = classicNamespace("CoState");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const AccountCoState: JazzClassicApiRemoved = classicNamespace("AccountCoState");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const InviteListener: JazzClassicApiRemoved = classicNamespace("InviteListener");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const SyncConnectionStatus: JazzClassicApiRemoved = classicNamespace("SyncConnectionStatus");
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const useAccountOrGuest: JazzClassicApiRemoved = classicFunction(
  "useAccountOrGuest",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const useJazzContext: JazzClassicApiRemoved = classicFunction(
  "useJazzContext",
) as unknown as JazzClassicApiRemoved;
/** @deprecated Jazz Classic (0.x), unsupported in Jazz 2. Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt. */
export const useAcceptInvite: JazzClassicApiRemoved = classicFunction(
  "useAcceptInvite",
) as unknown as JazzClassicApiRemoved;
