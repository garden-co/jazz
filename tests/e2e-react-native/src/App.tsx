import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import React, { StrictMode, useEffect, useRef } from "react";

import { JazzProvider, setupKvStore, useDemoAuth } from "jazz-react-native";
import { SimpleSharing } from "./screens/SimpleSharing";

const Stack = createNativeStackNavigator();

setupKvStore();

function App() {
  const [auth, state] = useDemoAuth();
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
  }, [state.state]);

  if (state.state === "ready" || !auth) {
    return null;
  }

  return (
    <StrictMode>
      <JazzProvider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=e2e-rn@garden.co"
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
