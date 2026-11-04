import { JazzProvider, useJazzAuth, useDb } from "./index.js";
function Data() {
  useDb();
  const { logout, retry, reauthenticate, user, status } = useJazzAuth();
  void logout;
  void retry;
  void reauthenticate;
  void user?.claims;
  return status;
}
export const expoApplication = (
  <JazzProvider
    scope="expo-app"
    serverUrl="https://sync.example.test"
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
  <JazzProvider serverUrl="https://app.example.test/jazz" initial="signed-out" signedOut={null}>
    <Data />
  </JazzProvider>
);
