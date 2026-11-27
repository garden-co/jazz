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
import type * as joinRequests from "../joinRequests.js";
import type * as lib_access from "../lib/access.js";
import type * as members from "../members.js";
import type * as messages from "../messages.js";
import type * as profiles from "../profiles.js";
import type * as reactions from "../reactions.js";
import type * as readMarkers from "../readMarkers.js";
import type * as rooms from "../rooms.js";
import type * as seed from "../seed.js";
import type * as strokes from "../strokes.js";
import type * as users from "../users.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  auth: typeof auth;
  http: typeof http;
  joinRequests: typeof joinRequests;
  "lib/access": typeof lib_access;
  members: typeof members;
  messages: typeof messages;
  profiles: typeof profiles;
  reactions: typeof reactions;
  readMarkers: typeof readMarkers;
  rooms: typeof rooms;
  seed: typeof seed;
  strokes: typeof strokes;
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
