export type Environment = "browser" | "mobile";
export type Engine = "browser" | "mobile" | "nodejs" | "deno" | "bun";
export type DocsFrameworks =
  | "react"
  | "svelte"
  | "react-native"
  | "react-native-expo";
export type Framework = "react" | "svelte" | "rn" | "expo" | "nextjs";
export type AuthMethod =
  | "minimal"
  | "passkey"
  | "passphrase"
  | "clerk"
  | "betterauth"
  | "multi";

export type EngineConfig = {
  [K in Engine]?: {
    [F in Framework]?: {
      auth: AuthMethod[];
    };
  };
};

export const frameworks: {
  name: string;
  value: Framework;
  docs: DocsFrameworks;
}[] = [
  {
    name: "React (Vite)",
    value: "react",
    docs: "react",
  },
  {
    name: "React (Next.js)",
    value: "nextjs",
    docs: "react",
  },
  {
    name: "React Native",
    value: "rn",
    docs: "react-native",
  },
  {
    name: "React Native (Expo)",
    value: "expo",
    docs: "react-native-expo",
  },
  {
    name: "Svelte",
    value: "svelte",
    docs: "svelte",
  },
];

export type ConfigStructure = Record<Environment, EngineConfig>;

export type ValidEngine<R extends Environment> = keyof NonNullable<
  ConfigStructure[R]
>;
export type ValidFramework<
  R extends Environment,
  E extends Engine,
> = keyof NonNullable<NonNullable<ConfigStructure[R]>[E]>;
export type ValidAuth<
  R extends Environment,
  E extends Engine,
  F extends Framework,
> = NonNullable<
  NonNullable<NonNullable<ConfigStructure[R]>[E]>[F]
>["auth"][number];

export const configMap: ConfigStructure = {
  browser: {
    browser: {
      react: { auth: ["minimal", "passkey", "passphrase", "clerk"] },
      svelte: { auth: ["passkey"] },
      nextjs: { auth: ["minimal"] },
    },
  },
  mobile: {
    mobile: {
      rn: { auth: ["minimal"] },
      expo: { auth: ["minimal"] },
    },
  },
};

export const PLATFORM = {
  WEB: "web",
  REACT_NATIVE: "react-native",
} as const;

export type FrameworkAuthPair =
  `${ValidFramework<Environment, ValidEngine<Environment>>}-${ValidAuth<Environment, ValidEngine<Environment>, ValidFramework<Environment, ValidEngine<Environment>>>}-auth`;

export const frameworkToAuthExamples: Partial<
  Record<
    FrameworkAuthPair,
    {
      name: string;
      repo: string | undefined;
      platform: (typeof PLATFORM)[keyof typeof PLATFORM];
    }
  >
> = {
  "react-passkey-auth": {
    name: "Passkey auth (easiest to start with)",
    repo: "garden-co/jazz/starters/react-passkey-auth",
    platform: PLATFORM.WEB,
  },
  "react-passphrase-auth": {
    name: "Passphrase auth",
    repo: "garden-co/jazz/examples/passphrase",
    platform: PLATFORM.WEB,
  },
  "react-clerk-auth": {
    name: "Clerk auth",
    repo: "garden-co/jazz/examples/clerk",
    platform: PLATFORM.WEB,
  },
  "react-multi-auth": {
    name: "Multi auth",
    repo: "garden-co/jazz/examples/multiauth",
    platform: PLATFORM.WEB,
  },
  "nextjs-betterauth-auth": {
    name: "BetterAuth",
    repo: "garden-co/jazz/examples/betterauth",
    platform: PLATFORM.WEB,
  },
  "nextjs-minimal-auth": {
    name: "Anonymous Auth",
    repo: "garden-co/jazz/examples/jazz-nextjs",
    platform: PLATFORM.WEB,
  },
  "svelte-passkey-auth": {
    name: "Passkey auth",
    repo: "garden-co/jazz/starters/svelte-passkey-auth",
    platform: PLATFORM.WEB,
  },
  "svelte-minimal-auth": {
    name: "Anonymous Auth with SvelteKit",
    repo: "garden-co/jazz/examples/jazz-sveltekit",
    platform: PLATFORM.WEB,
  },
  "rn-minimal-auth": {
    name: "Anonymous auth",
    repo: "garden-co/jazz/examples/chat-rn",
    platform: PLATFORM.REACT_NATIVE,
  },
  "expo-minimal-auth": {
    name: "Anonymous auth",
    repo: "garden-co/jazz/examples/chat-rn-expo",
    platform: PLATFORM.REACT_NATIVE,
  },
};

export type RuntimeEngines = typeof configMap;
export type Runtime = keyof RuntimeEngines;
