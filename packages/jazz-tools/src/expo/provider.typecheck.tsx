import { JazzProvider, jwtAuth, useJazzAuth, useDb } from "./index.js";
function Data() {
  useDb();
  const { logout, retry, status } = useJazzAuth();
  void logout;
  void retry;
  return status;
}
export const expoApplication = (
  <JazzProvider
    appId="expo-app"
    serverUrl="https://sync.example.test"
    auth={jwtAuth({ key: null, getToken: async () => "token", logout: async () => {} })}
    signedOut={null}
    error={({ retry }) => {
      void retry;
      return null;
    }}
  >
    <Data />
  </JazzProvider>
);
