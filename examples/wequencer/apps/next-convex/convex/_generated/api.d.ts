/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as auth from "../auth.js";
import type * as http from "../http.js";
import type * as lib_access from "../lib/access.js";
import type * as lib_setup from "../lib/setup.js";
import type * as members from "../members.js";
import type * as patterns from "../patterns.js";
import type * as presence from "../presence.js";
import type * as seed from "../seed.js";
import type * as sessions from "../sessions.js";
import type * as steps from "../steps.js";
import type * as tracks from "../tracks.js";
import type * as transport from "../transport.js";
import type * as users from "../users.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  auth: typeof auth;
  http: typeof http;
  "lib/access": typeof lib_access;
  "lib/setup": typeof lib_setup;
  members: typeof members;
  patterns: typeof patterns;
  presence: typeof presence;
  seed: typeof seed;
  sessions: typeof sessions;
  steps: typeof steps;
  tracks: typeof tracks;
  transport: typeof transport;
  users: typeof users;
}>;

/**
 * A utility for referencing Convex functions in your app's public API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
export declare const api: FilterApi<
  typeof fullApi,
  FunctionReference<any, "public">
>;

/**
 * A utility for referencing Convex functions in your app's internal API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = internal.myModule.myFunction;
 * ```
 */
export declare const internal: FilterApi<
  typeof fullApi,
  FunctionReference<any, "internal">
>;

export declare const components: {};
