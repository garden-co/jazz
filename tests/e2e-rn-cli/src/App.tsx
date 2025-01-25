import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import React, { StrictMode, useEffect, useRef } from "react";

import {
  JazzProvider,
  clearUserCredentials,
  setupKvStore,
  useDemoAuth,
} from "jazz-react-native";
import { MMKVStore } from "./mmkv-store";
import { SimpleSharing } from "./screens/SimpleSharing";

const Stack = createNativeStackNavigator();

const auth_store = new MMKVStore();
// auth_store.clearAll();
// clearUserCredentials();

function App() {
  setupKvStore(auth_store);

  const [auth, state] = useDemoAuth({ store: auth_store });
  const navigationRef = useNavigationContainerRef();
  const signedUp = useRef(false);

  useEffect(() => {
    if (state.state === "ready" && !signedUp.current) {
      if (state.existingUsers.includes("MisterX")) {
        state.logInAs("MisterX");
      } else {
        state.signUp("MisterX");
      }

      signedUp.current = true;
    }
  }, [state]);

  if (state.state === "ready" || !auth) {
    return null;
  }

  return (
    <StrictMode>
      <JazzProvider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=e2e-rn-cli@garden.co"
      >
        <NavigationContainer ref={navigationRef}>
          <Stack.Navigator initialRouteName="SimpleSharing">
            <Stack.Screen name="SimpleSharing" component={SimpleSharing} />
          </Stack.Navigator>
        </NavigationContainer>
      </JazzProvider>
    </StrictMode>
  );
}

export default App;
