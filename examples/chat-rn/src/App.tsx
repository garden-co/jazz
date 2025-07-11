import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import {
  args_return_ab,
  args_return_string,
  no_args_return_ab,
  no_args_return_string,
} from "jazz-crypto";
import { JazzReactNativeProvider } from "jazz-tools/react-native";
import React, { StrictMode, useEffect, useState } from "react";
import { Linking } from "react-native";
import { apiKey } from "./apiKey";
import { ChatScreen } from "./chat";
import { HandleInviteScreen } from "./invite";
import { theme } from "./theme";

type RootStackParamList = {
  ChatScreen: undefined;
  HandleInviteScreen: undefined;
};

const Stack = createNativeStackNavigator<RootStackParamList>();

function App() {
  const [initialRoute, setInitialRoute] = useState<
    "ChatScreen" | "HandleInviteScreen"
  >("ChatScreen");
  const navigationRef = useNavigationContainerRef();

  useEffect(() => {
    // test crypto
    try {
      const ret1 = no_args_return_string();
      console.log(ret1);
      const ret2 = args_return_string("test");
      console.log(ret2);
      const ret3 = no_args_return_ab();
      console.log(ret3);
      const ret4 = args_return_ab(new ArrayBuffer(1));
      console.log(ret4);
    } catch (e) {
      console.error(e);
    }
  }, []);

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
