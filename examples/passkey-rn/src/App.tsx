import React, { StrictMode } from "react";
import { useColorScheme } from "react-native";
import {
  NavigationContainer,
  DefaultTheme,
  DarkTheme,
} from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { SafeAreaProvider } from "react-native-safe-area-context";

import { JazzReactNativeProvider } from "jazz-tools/react-native";
import { RNCrypto } from "jazz-tools/react-native-core/crypto/RNCrypto";

import { apiKey } from "./apiKey";
import { AuthScreen } from "./AuthScreen";
import { NotesScreen } from "./NotesScreen";

type RootStackParamList = {
  Auth: undefined;
  Notes: undefined;
};

const Stack = createNativeStackNavigator<RootStackParamList>();

export default function App() {
  const colorScheme = useColorScheme();

  return (
    <StrictMode>
      <SafeAreaProvider>
        <JazzReactNativeProvider
          sync={{
            peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
          }}
          CryptoProvider={RNCrypto}
        >
          <NavigationContainer
            theme={colorScheme === "dark" ? DarkTheme : DefaultTheme}
          >
            <Stack.Navigator
              initialRouteName="Auth"
              screenOptions={{ headerShown: false }}
            >
              <Stack.Screen name="Auth" component={AuthScreen} />
              <Stack.Screen name="Notes" component={NotesScreen} />
            </Stack.Navigator>
          </NavigationContainer>
        </JazzReactNativeProvider>
      </SafeAreaProvider>
    </StrictMode>
  );
}
