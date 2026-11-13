/**
 * @internal What a host's `createJazzClient` (and `<JazzProvider>`) accepts,
 * checked when it is called. A missing config or an option it doesn't take is
 * refused by name, listing the options it does take: an unknown option left
 * in would be ignored, quietly changing what the app asked for (an `auth` or
 * `guest` option from another library, or a typo).
 */

import { refuseRemovedSessionOptions } from "./jazz-session.js";

/**
 * Names apps reach for that this session has no option for, with what
 * replaced them. Each is still refused; the hint says what to use instead.
 */
const REPLACED_OPTIONS: Record<string, string> = {
  auth: "your backend decides who is signed in (createJazz's `authenticate`); call `session.refresh()` after your own sign-in or sign-out",
  authProvider:
    "your backend decides who is signed in (createJazz's `authenticate`); call `session.refresh()` after your own sign-in or sign-out",
  guest: 'guests are device accounts: use `initial: "device"`',
  guestMode: 'guests are device accounts: use `initial: "device"`',
  anonymous: 'guests are device accounts: use `initial: "device"`',
  sync: "the endpoint the client syncs with is `serverUrl`",
  peer: "the endpoint the client syncs with is `serverUrl`",
  syncUrl: "the endpoint the client syncs with is `serverUrl`",
  url: "the endpoint the client syncs with is `serverUrl`",
  storage: 'local storage is `driver` (`{ type: "persistent" }` or `{ type: "memory" }`)',
  localFirstAuth:
    'device accounts replaced local-first auth: use `initial: "device"` and the session\'s `createDeviceAccount()`',
};

/**
 * Refuse a config that is missing or not an object, an option alpha.60 took
 * (by its own message), and any other option not in `options`. `options` is
 * the host's whole option list, kept in step with its config type by
 * `satisfies Record<keyof Config, true>` where it is declared.
 */
export function refuseInvalidSessionConfig(
  config: unknown,
  options: Readonly<Record<string, true>>,
  call = "createJazzClient",
): void {
  const names = Object.keys(options).sort();
  const list = `Options: ${names.join(", ")}.`;
  if (config === null || typeof config !== "object" || Array.isArray(config)) {
    throw new TypeError(
      `${call} needs a config object, e.g. ${call}({ serverUrl: "/jazz" }), or ${call}({}) for the defaults; got ${
        config === undefined ? "nothing" : config === null ? "null" : typeof config
      }. ${list}`,
    );
  }
  refuseRemovedSessionOptions(config);
  for (const key of Object.keys(config)) {
    if (Object.hasOwn(options, key)) continue;
    const value = (config as Record<string, unknown>)[key];
    if (typeof value === "function") {
      // A function or class: no option takes one (the browser's WebSocket is
      // the platform's own).
      throw new TypeError(
        `${call} has no option "${key}" that takes a function or class` +
          (key === "WebSocket"
            ? ": the client uses the platform's own WebSocket (Node has one from 22)"
            : ""),
      );
    }
    const replaced = Object.hasOwn(REPLACED_OPTIONS, key) ? `: ${REPLACED_OPTIONS[key]}` : "";
    throw new TypeError(`${call} has no option "${key}"${replaced}. ${list}`);
  }
}
