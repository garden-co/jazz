import "../global.css";

import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import * as Linking from "expo-linking";
import React, { StrictMode, useEffect, useState } from "react";
import HandleInviteScreen from "./invite";

import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react-native";
import ChatScreen from "./chat";

const Stack = createNativeStackNavigator();

const prefix = Linking.createURL("/");

const linking = {
  prefixes: [prefix],
  config: {
    screens: {
      HandleInviteScreen: {
        path: "router/invite/:valueHint?/:valueID/:inviteSecret",
      },
    },
  },
};

function App() {
  const [auth, state] = useDemoAuth();
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

  if (!auth) {
    return null;
  }

  return (
    <StrictMode>
      <JazzProvider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=chat-rn-example-jazz@garden.co"
      >
        <NavigationContainer linking={linking} ref={navigationRef}>
          <Stack.Navigator initialRouteName={initialRoute}>
            <Stack.Screen
              options={{ title: "Jazz Chat" }}
              name="ChatScreen"
              // @ts-ignore
              component={ChatScreen}
            />
            <Stack.Screen
              name="HandleInviteScreen"
              component={HandleInviteScreen}
            />
          </Stack.Navigator>
        </NavigationContainer>
      </JazzProvider>
      {state.state !== "signedIn" ? (
        <DemoAuthBasicUI appName="Jazz Chat" state={state} />
      ) : null}
    </StrictMode>
  );
}

export default App;
