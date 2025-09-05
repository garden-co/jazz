import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { JazzReactNativeProvider } from "jazz-tools/react-native";
import { RNCrypto } from "jazz-tools/react-native-core/crypto";
import React, { StrictMode, useEffect, useState } from "react";
import { Linking, LogBox } from "react-native";
import { apiKey } from "./apiKey";
import { ChatScreen } from "./chat";
import { HandleInviteScreen } from "./invite";
import { theme } from "./theme";

type RootStackParamList = {
  ChatScreen: undefined;
  HandleInviteScreen: undefined;
};

// Create the stack navigator with proper typing
const Stack = createNativeStackNavigator<RootStackParamList>();

function App() {
  const [initialRoute, setInitialRoute] = useState<
    "ChatScreen" | "HandleInviteScreen"
  >("ChatScreen");
  const navigationRef = useNavigationContainerRef();
  useEffect(() => {
    Linking.getInitialURL().then((url) => {
      if (url) {
        if (url && url.includes("invite")) {
          setInitialRoute("HandleInviteScreen");
        }
      }
    });
  }, []);

  return (
    <StrictMode>
      <JazzReactNativeProvider
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
        CryptoProvider={RNCrypto}
      >
        <NavigationContainer ref={navigationRef} theme={theme}>
          <Stack.Navigator initialRouteName={initialRoute}>
            <Stack.Screen
              options={{ title: "Jazz Chat" }}
              name="ChatScreen"
              component={ChatScreen}
            />
            <Stack.Screen
              name="HandleInviteScreen"
              component={HandleInviteScreen}
            />
          </Stack.Navigator>
        </NavigationContainer>
      </JazzReactNativeProvider>
    </StrictMode>
  );
}

export default App;

LogBox.ignoreLogs(["Open debugger to view warnings"]);
