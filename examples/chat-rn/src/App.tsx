import {
  NavigationContainer,
  useNavigationContainerRef,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { JazzProvider } from "jazz-react-native";
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

// Create the stack navigator with proper typing
const Stack = createNativeStackNavigator<RootStackParamList>();

// const prefix = Linking.createURL("/");

// const linking = {
//   prefixes: [prefix],
//   config: {
//     screens: {
//       HandleInviteScreen: {
//         path: "router/invite/:valueHint?/:valueID/:inviteSecret",
//       },
//     },
//   },
// };

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
      </JazzProvider>
    </StrictMode>
  );
}

export default App;
