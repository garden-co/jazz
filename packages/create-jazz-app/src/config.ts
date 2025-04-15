export type Environment = "browser" | "mobile";
export type Engine = "browser" | "mobile" | "nodejs" | "deno" | "bun";
export type Framework = "react" | "vue" | "svelte" | "rn";
export type AuthMethod =
  | "demo"
  | "passkey"
  | "passphrase"
  | "clerk"
  | "keypair";

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
}[] = [
  {
    name: "React",
    value: "react",
  },
  {
    name: "React Native",
    value: "rn",
  },
  {
    name: "Svelte",
    value: "svelte",
  },
  {
    name: "Vue",
    value: "vue",
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
      react: { auth: ["demo", "passkey", "passphrase", "clerk"] },
      vue: { auth: ["demo"] },
      svelte: { auth: ["passkey"] },
    },
  },
  mobile: {
    mobile: {
      rn: { auth: ["demo", "clerk"] },
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
  "react-clerk-auth": {
    name: "Clerk auth",
    repo: "garden-co/jazz/examples/clerk",
    platform: PLATFORM.WEB,
  },
  "vue-demo-auth": {
    name: "Demo auth",
    repo: "garden-co/jazz/examples/todo-vue",
    platform: PLATFORM.WEB,
  },
  "svelte-passkey-auth": {
    name: "Passkey auth",
    repo: "garden-co/jazz/examples/passkey-svelte",
    platform: PLATFORM.WEB,
  },
  "rn-clerk-auth": {
    name: "Clerk auth",
    repo: "garden-co/jazz/examples/chat-rn-expo-clerk",
    platform: PLATFORM.REACT_NATIVE,
  },
};

export type RuntimeEngines = typeof configMap;
export type Runtime = keyof RuntimeEngines;
