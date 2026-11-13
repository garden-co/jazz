import { JazzProvider, useJazzSession, useDb, useSyncStatus, type SyncStatus } from "./index.js";
function Data() {
  useDb();
  useSyncStatus() satisfies SyncStatus;
  const { signOut, retry, refresh, user, status } = useJazzSession();
  void signOut;
  void retry;
  void refresh;
  void user?.claims;
  void user?.identity.subject;
  return status;
}
export const expoApplication = (
  <JazzProvider
    config={{ scope: "expo-app", serverUrl: "https://sync.example.test" }}
    signedOut={null}
    error={({ retry }) => {
      void retry;
      return null;
    }}
  >
    <Data />
  </JazzProvider>
);
export const expoApplicationWithoutGuests = (
  <JazzProvider
    config={{ serverUrl: "https://app.example.test/jazz", initial: "signed-out" }}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
export const expoApplicationWithBearer = (
  <JazzProvider
    config={{ serverUrl: "https://app.example.test/jazz", bearer: async () => "app-token" }}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
