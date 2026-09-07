declare const classicApiRemoved: unique symbol;

/** An unsupported Jazz Classic value, deliberately neither callable nor constructable. */
interface JazzClassicApiRemoved {
  readonly [classicApiRemoved]: never;
}

function failClassicApi(operation: string): never {
  const error = new Error(
    `[JAZZ_CLASSIC_API_REMOVED] ${operation} belongs to Jazz Classic (0.x) ` +
      `and is not supported by this Jazz 2 package.\n\n` +
      `Jazz 2 uses relational tables, queries, and row-level permission policies. ` +
      `This is not a rename-only migration.\n\n` +
      `Read node_modules/jazz-tools/README.md or https://jazz.tools/llms-full.txt before continuing.`,
  );
  throw Object.assign(error, { code: "JAZZ_CLASSIC_API_REMOVED" });
}

function classicFunction(name: string) {
  // Keep ordinary function metadata: React may inspect it before rendering.
  return function removedClassicApi(): never {
    return failClassicApi(name);
  };
}

function classicNamespace(name: string): JazzClassicApiRemoved {
  // A constructable target also catches `new CoMap()` and `extends CoMap`.
  // Calls/construction reach the throwing function; property reads fail here.
  return new Proxy(classicFunction(name), {
    get(_target, property) {
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
