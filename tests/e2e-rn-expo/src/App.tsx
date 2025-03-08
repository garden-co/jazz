import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import React, { StrictMode } from "react";

import { JazzProvider } from "jazz-expo";
import { SimpleSharing } from "./screens/SimpleSharing";

const Stack = createNativeStackNavigator();

function App() {
  const navigationRef = useNavigationContainerRef();

  return (
    <StrictMode>
      <JazzProvider
        sync={{
          peer: "wss://cloud.jazz.tools/?key=e2e-rn@garden.co",
        }}
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
