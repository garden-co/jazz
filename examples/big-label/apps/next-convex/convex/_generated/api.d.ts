/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as artists from "../artists.js";
import type * as auth from "../auth.js";
import type * as bootstrap from "../bootstrap.js";
import type * as catalogues from "../catalogues.js";
import type * as demo from "../demo.js";
import type * as http from "../http.js";
import type * as lib_access from "../lib/access.js";
import type * as lib_paging from "../lib/paging.js";
import type * as members from "../members.js";
import type * as organizations from "../organizations.js";
import type * as overview from "../overview.js";
import type * as releaseTeams from "../releaseTeams.js";
import type * as releases from "../releases.js";
import type * as seed from "../seed.js";
import type * as teams from "../teams.js";
import type * as users from "../users.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  artists: typeof artists;
  auth: typeof auth;
  bootstrap: typeof bootstrap;
  catalogues: typeof catalogues;
  demo: typeof demo;
  http: typeof http;
  "lib/access": typeof lib_access;
  "lib/paging": typeof lib_paging;
  members: typeof members;
  organizations: typeof organizations;
  overview: typeof overview;
  releaseTeams: typeof releaseTeams;
  releases: typeof releases;
  seed: typeof seed;
  teams: typeof teams;
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
