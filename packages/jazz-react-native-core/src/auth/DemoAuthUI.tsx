import { useDemoAuth } from "jazz-react-core";
import React, { useState } from "react";
import {
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useColorScheme,
} from "react-native";

export const DemoAuthBasicUI = ({
  appName,
  auth,
  children,
}: {
  appName: string;
  auth: ReturnType<typeof useDemoAuth>;
  children: React.ReactNode;
}) => {
  const colorScheme = useColorScheme();
  const darkMode = colorScheme === "dark";
  const [username, setUsername] = useState<string>("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const handleSignUp = () => {
    setErrorMessage(null);

    auth.signUp(username).catch((error) => {
      setErrorMessage(error.message);
    });
  };

  const handleLogIn = (username: string) => {
    setErrorMessage(null);

    auth.logIn(username).catch((error) => {
      setErrorMessage(error.message);
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
          placeholderTextColor={darkMode ? "#fff" : "#000"}
          style={[
            styles.textInput,
            darkMode ? styles.darkInput : styles.lightInput,
          ]}
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
            Sign Up as new account
          </Text>
        </TouchableOpacity>

        <View style={styles.existingUsersContainer}>
          {auth.existingUsers.map((user) => (
            <TouchableOpacity
              key={user}
              onPress={() => handleLogIn(user)}
              style={[
                styles.existingUserButton,
                darkMode ? styles.darkUserButton : styles.lightUserButton,
              ]}
            >
              <Text style={darkMode ? styles.darkText : styles.lightText}>
                Log In as "{user}"
              </Text>
            </TouchableOpacity>
          ))}
        </View>
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
    alignItems: "center",
    justifyContent: "center",
  },
  headerText: {
    fontSize: 24,
    marginBottom: 20,
  },
  errorText: {
    color: "red",
    marginVertical: 5,
    textAlign: "center",
  },
  textInput: {
    borderWidth: 1,
    padding: 10,
    marginVertical: 10,
    width: "100%",
    borderRadius: 6,
  },
  darkInput: {
    borderColor: "#444",
    backgroundColor: "#000",
    color: "#fff",
  },
  lightInput: {
    borderColor: "#ddd",
    backgroundColor: "#fff",
    color: "#000",
  },
  button: {
    paddingVertical: 15,
    paddingHorizontal: 10,
    borderRadius: 6,
    width: "100%",
    marginVertical: 10,
  },
  darkButton: {
    backgroundColor: "#444",
  },
  lightButton: {
    backgroundColor: "#ddd",
  },
  darkButtonText: {
    color: "#fff",
    textAlign: "center",
  },
  lightButtonText: {
    color: "#000",
    textAlign: "center",
  },
  existingUsersContainer: {
    width: "100%",
    marginTop: 20,
  },
  existingUserButton: {
    paddingVertical: 15,
    paddingHorizontal: 10,
    borderRadius: 6,
    marginVertical: 5,
  },
  darkUserButton: {
    backgroundColor: "#222",
  },
  lightUserButton: {
    backgroundColor: "#eee",
  },
  loadingText: {
    fontSize: 18,
    color: "#888",
  },
  darkText: {
    color: "#fff",
  },
  lightText: {
    color: "#000",
  },
  darkBackground: {
    backgroundColor: "#000",
  },
  lightBackground: {
    backgroundColor: "#fff",
  },
});
