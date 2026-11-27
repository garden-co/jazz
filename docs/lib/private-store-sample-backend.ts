// What the directory store sample (`examples/docs/e2ee-ts/src/directory-store.ts`)
// imports from `jazz-tools/backend`, taken from source. The docs install
// doesn't build jazz-tools, so `tsconfig.json` and `vitest.config.ts` resolve
// `jazz-tools/backend` here, for `private-store.test.ts`.
export {
  PrivateStoreConflictError,
  PrivateStoreWipedError,
} from "../../packages/jazz-tools/src/encryption/private-store-errors";
export type { PrivateStore } from "../../packages/jazz-tools/src/encryption/private-store";
