/**
 * The native addon does not currently publish TypeScript declarations. Keep
 * its small locking API here so every package typecheck can import it safely.
 */
declare module "fs-native-extensions" {
  export function waitForLockSync(descriptor: number): void;
  export function unlock(descriptor: number): void;
}
