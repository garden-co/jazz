import "../global.css";

import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import * as Linking from "expo-linking";
import React, { StrictMode, useEffect, useState } from "react";
import HandleInviteScreen from "./invite";

import { JazzProvider } from "jazz-expo";
import { apiKey } from "./apiKey";
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
      <JazzProvider
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
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
    </StrictMode>
  );
}

export default App;
