import React, { useState } from "react";
import {
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useColorScheme,
} from "react-native";
import { usePasskeyAuth } from "./usePasskeyAuth.js";

/**
 * A basic UI component for passkey authentication in React Native apps.
 *
 * This component provides a simple sign-up and log-in interface using passkeys.
 * It's designed for quick prototyping and can be customized or replaced with
 * your own authentication UI.
 *
 * @example
 * ```tsx
 * import { PasskeyAuthBasicUI } from "jazz-tools/react-native-core";
 *
 * function App() {
 *   return (
 *     <JazzProvider ...>
 *       <PasskeyAuthBasicUI
 *         appName="My App"
 *         rpId="myapp.com"
 *       >
 *         <MainApp />
 *       </PasskeyAuthBasicUI>
 *     </JazzProvider>
 *   );
 * }
 * ```
 *
 * @category Auth Providers
 */
export const PasskeyAuthBasicUI = ({
  appName,
  rpId,
  children,
}: {
  appName: string;
  rpId: string;
  children: React.ReactNode;
}) => {
  const colorScheme = useColorScheme();
  const darkMode = colorScheme === "dark";
  const [username, setUsername] = useState<string>("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const auth = usePasskeyAuth({ appName, rpId });

  const handleSignUp = () => {
    setErrorMessage(null);

    auth.signUp(username).catch((error) => {
      if (error.cause instanceof Error) {
        setErrorMessage(error.cause.message);
      } else {
        setErrorMessage(error.message);
      }
    });
  };

  const handleLogIn = () => {
    setErrorMessage(null);

    auth.logIn().catch((error) => {
      if (error.cause instanceof Error) {
        setErrorMessage(error.cause.message);
      } else {
        setErrorMessage(error.message);
      }
    });
  };

  if (auth.state === "signedIn") {
    return children;
  }

  return (
    <View
      style={[
        styles.container,
        darkMode ? styles.darkBackground : styles.lightBackground,
      ]}
    >
      <View style={styles.formContainer}>
        <Text
          style={[
            styles.headerText,
            darkMode ? styles.darkText : styles.lightText,
          ]}
        >
          {appName}
        </Text>

        {errorMessage && <Text style={styles.errorText}>{errorMessage}</Text>}

        <TextInput
          placeholder="Display name"
          value={username}
          onChangeText={setUsername}
          placeholderTextColor={darkMode ? "#999" : "#666"}
          style={[
            styles.textInput,
            darkMode ? styles.darkInput : styles.lightInput,
          ]}
          autoCapitalize="words"
          autoCorrect={false}
        />

        <TouchableOpacity
          onPress={handleSignUp}
          style={[
            styles.button,
            darkMode ? styles.darkButton : styles.lightButton,
          ]}
        >
          <Text
            style={darkMode ? styles.darkButtonText : styles.lightButtonText}
          >
            Sign Up with Passkey
          </Text>
        </TouchableOpacity>

        <View style={styles.divider}>
          <View
            style={[
              styles.dividerLine,
              darkMode ? styles.darkDivider : styles.lightDivider,
            ]}
          />
          <Text
            style={[
              styles.dividerText,
              darkMode ? styles.darkText : styles.lightText,
            ]}
          >
            or
          </Text>
          <View
            style={[
              styles.dividerLine,
              darkMode ? styles.darkDivider : styles.lightDivider,
            ]}
          />
        </View>

        <TouchableOpacity
          onPress={handleLogIn}
          style={[
            styles.secondaryButton,
            darkMode ? styles.darkSecondaryButton : styles.lightSecondaryButton,
          ]}
        >
          <Text style={darkMode ? styles.darkText : styles.lightText}>
            Log In with Existing Passkey
          </Text>
        </TouchableOpacity>
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    padding: 20,
  },
  formContainer: {
    width: "80%",
    maxWidth: 300,
    alignItems: "center",
    justifyContent: "center",
  },
  headerText: {
    fontSize: 24,
    fontWeight: "600",
    marginBottom: 30,
  },
  errorText: {
    color: "#ff4444",
    marginVertical: 10,
    textAlign: "center",
    fontSize: 14,
  },
  textInput: {
    borderWidth: 1,
    padding: 12,
    marginVertical: 10,
    width: "100%",
    borderRadius: 8,
    fontSize: 16,
  },
  darkInput: {
    borderColor: "#444",
    backgroundColor: "#1a1a1a",
    color: "#fff",
  },
  lightInput: {
    borderColor: "#ddd",
    backgroundColor: "#fff",
    color: "#000",
  },
  button: {
    paddingVertical: 14,
    paddingHorizontal: 10,
    borderRadius: 8,
    width: "100%",
    marginVertical: 10,
  },
  darkButton: {
    backgroundColor: "#0066cc",
  },
  lightButton: {
    backgroundColor: "#007aff",
  },
  darkButtonText: {
    color: "#fff",
    textAlign: "center",
    fontWeight: "600",
    fontSize: 16,
  },
  lightButtonText: {
    color: "#fff",
    textAlign: "center",
    fontWeight: "600",
    fontSize: 16,
  },
  divider: {
    flexDirection: "row",
    alignItems: "center",
    width: "100%",
    marginVertical: 20,
  },
  dividerLine: {
    flex: 1,
    height: 1,
  },
  darkDivider: {
    backgroundColor: "#444",
  },
  lightDivider: {
    backgroundColor: "#ddd",
  },
  dividerText: {
    marginHorizontal: 10,
    fontSize: 14,
  },
  secondaryButton: {
    paddingVertical: 14,
    paddingHorizontal: 10,
    borderRadius: 8,
    width: "100%",
    borderWidth: 1,
  },
  darkSecondaryButton: {
    borderColor: "#444",
    backgroundColor: "transparent",
  },
  lightSecondaryButton: {
    borderColor: "#ddd",
    backgroundColor: "transparent",
  },
  darkText: {
    color: "#fff",
    textAlign: "center",
  },
  lightText: {
    color: "#000",
    textAlign: "center",
  },
  darkBackground: {
    backgroundColor: "#000",
  },
  lightBackground: {
    backgroundColor: "#fff",
  },
});
