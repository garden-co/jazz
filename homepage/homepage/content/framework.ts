// If we're updating here, don't forget to update in next.config.mjs as well

export enum Framework {
  React = "react",
  ReactNative = "react-native",
  ReactNativeExpo = "react-native-expo",
  Svelte = "svelte",
  Vanilla = "vanilla",
}

export const frameworks = Object.values(Framework);
export const frameworkNames: Record<
  Framework,
  {
    label: string;
    experimental: boolean;
    hidden?: boolean;
  }
> = {
  [Framework.React]: {
    label: "React",
    experimental: false,
  },
  [Framework.ReactNative]: {
    label: "React Native",
    experimental: false,
  },
  [Framework.ReactNativeExpo]: {
    label: "React Native (Expo)",
    experimental: false,
  },
  [Framework.Svelte]: {
    label: "Svelte",
    experimental: false,
  },
  [Framework.Vanilla]: {
    label: "VanillaJS",
    experimental: false,
  },
};

export const DEFAULT_FRAMEWORK = Framework.React;

export function isValidFramework(value: string): value is Framework {
  return frameworks.includes(value as Framework);
}
