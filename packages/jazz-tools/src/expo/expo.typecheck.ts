// Typecheck-only: the Expo entry's session API.
import {
  createJazzClient,
  useJazzSession,
  useSyncStatus,
  type SyncStatus,
  type JazzClientConfig,
  type JazzUser,
} from "./index.js";

// The app's own token for its backend, passed through to the session.
const withBearer: JazzClientConfig = {
  serverUrl: "https://app.example.test/jazz",
  bearer: () => "app-token",
};
void createJazzClient(withBearer).then((session) => {
  const user: JazzUser | null = session.getSnapshot().user;
  void user?.accountId;
  return session.createDeviceAccount();
});

// The session and sync hooks are on the Expo entry too, not only on react-native.
export function useSessionAndSync() {
  const session = useJazzSession();
  return { user: session.user, sync: useSyncStatus() satisfies SyncStatus };
}
