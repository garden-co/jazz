export enum Framework {
  React = "react",
  ReactNative = "react-native",
  ReactNativeExpo = "react-native-expo",
  Vue = "vue",
  Svelte = "svelte",
  Vanilla = "vanilla",
}

export const frameworks = Object.values(Framework);

export const DEFAULT_FRAMEWORK = Framework.React;

export function isValidFramework(value: string): value is Framework {
  return frameworks.includes(value as Framework);
}
