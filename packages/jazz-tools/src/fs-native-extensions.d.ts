declare module "fs-native-extensions" {
  export function waitForLockSync(descriptor: number): void;
  export function unlock(descriptor: number): void;
}
