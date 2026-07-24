# Storage, networking, and native lifecycle

## Secure identity storage

`ExpoAuthSecretStore` stores a generated local-first secret through `expo-secure-store` and uses
`expo-crypto` for randomness. The same secret produces the same Jazz identity.

Keep these operations distinct:

- clearing the SecureStore secret changes or removes identity access;
- deleting the native database removes cached/offline rows;
- logging out an external provider clears its own credential;
- uninstalling the app may have platform-specific effects on each storage system.

Use `jazz-auth` before implementing account switching, recovery, or a local-first-to-external
upgrade.

## Persistent row storage

`jazz-rn` accepts `dataPath` through the React Native database config. Inspect the installed native
runtime: versions in this repository open SQLite and otherwise derive a file under the process
temporary directory. A temporary default is not a durable offline-storage contract.

Choose a stable file inside application-owned persistent storage using the installed
`expo-file-system` API or the native project's equivalent. Pass a decoded native filesystem path,
not an unexamined `file://` URI. Keep the path stable for the app ID and user-storage policy.

Verify persistence behavior, not just configuration:

1. sync a row;
2. stop the sync server;
3. terminate and cold-relaunch the app;
4. confirm the row is readable locally;
5. make an offline write;
6. reconnect and confirm its edge wait settles.

## Development sync URLs

Typical host mappings are:

| Target                  | Development server URL                     |
| ----------------------- | ------------------------------------------ |
| iOS simulator           | `http://127.0.0.1:1625`                    |
| Android Studio emulator | `http://10.0.2.2:1625`                     |
| Physical device         | `http://<development-machine-LAN-IP>:1625` |

Genymotion and other runtimes can use different host aliases. Inspect the actual environment.

The development helper may inject a host-loopback URL. Verify whether the installed application or
plugin rewrites it for Android and physical devices; if not, resolve the URL in app configuration or
connect the helper to a separately reachable server.

For physical devices:

- bind the development server beyond loopback;
- keep phone and host on a reachable network;
- allow the port through the host firewall;
- configure iOS local-network/transport policy and Android cleartext-development policy as needed;
- use HTTPS outside controlled local development.

Never expose the Jazz admin or backend secret through `EXPO_PUBLIC_*` merely to let a device reach
the server.

## Lifecycle and background behavior

`JazzProvider` owns its client and releases it when the last matching provider unmounts. Keep its
config stable and do not shut its database down manually.

If application code creates a client directly, call `shutdown()` when that owner terminates. Test
fast refresh, logout, and remount behavior so stale native clients do not retain the wrong identity.

Do not promise continued sync while iOS or Android suspends the process. Unless the app has an
explicit native background task with verified runtime support, describe behavior as foreground sync
plus reconciliation after the app resumes.
