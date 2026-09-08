import { JazzProvider, jwtAuth, useJazzAuth } from "./index.js";
import type { AccountStore } from "../accounts/persistence.js";
declare const store: AccountStore;
function Data() {
  const { status, sessionActions } = useJazzAuth();
  void sessionActions;
  return status;
}
export const nativeApplication = (
  <JazzProvider
    appId="native-app"
    serverUrl="https://sync.example.test"
    store={store}
    auth={jwtAuth({ key: null, getToken: async () => "token", logout: async () => {} })}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
