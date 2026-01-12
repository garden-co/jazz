import React, { useEffect, useState } from "react";
import {
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useColorScheme,
} from "react-native";
import {
  usePasskeyAuth,
  isPasskeySupported,
} from "jazz-tools/react-native-core";

type Props = {
  navigation: any;
};

/**
 * Authentication screen using passkey (WebAuthn) authentication.
 *
 * NOTE: For passkeys to work, you must:
 * 1. Replace "example.com" with your actual domain
 * 2. Configure Associated Domains on iOS (webcredentials:yourdomain.com)
 * 3. Host an AASA file at https://yourdomain.com/.well-known/apple-app-site-association
 * 4. Configure assetlinks.json on Android
 */
export function AuthScreen({ navigation }: Props) {
  const colorScheme = useColorScheme();
  const darkMode = colorScheme === "dark";
  const [username, setUsername] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isSupported, setIsSupported] = useState<boolean | null>(null);

  // Check passkey support on mount
  useEffect(() => {
    isPasskeySupported().then(setIsSupported);
  }, []);

  // TODO: Replace with your domain that has AASA/assetlinks configured
  const auth = usePasskeyAuth({
    appName: "Jazz Passkey Demo",
    rpId: "example.com", // Change this to your domain
  });

  useEffect(() => {
    if (auth.state === "signedIn") {
      navigation.replace("Notes");
    }
  }, [auth.state, navigation]);

  const handleSignUp = async () => {
    setError(null);
    try {
      await auth.signUp(username);
    } catch (e: any) {
      setError(e.cause?.message || e.message);
    }
  };

  const handleLogIn = async () => {
    setError(null);
    try {
      await auth.logIn();
    } catch (e: any) {
      setError(e.cause?.message || e.message);
    }
  };

  return (
    <View style={[styles.container, darkMode ? styles.darkBg : styles.lightBg]}>
      <View style={styles.authContainer}>
        <Text
          style={[styles.title, darkMode ? styles.darkText : styles.lightText]}
        >
          Jazz Passkey Demo
        </Text>

        <Text
          style={[
            styles.subtitle,
            darkMode ? styles.darkSubtext : styles.lightSubtext,
          ]}
        >
          Secure authentication with device biometrics
        </Text>

        {isSupported === false && (
          <View style={styles.warningBox}>
            <Text style={styles.warningText}>
              Passkeys are not supported on this device. You may need to run on
              a physical device with biometric authentication.
            </Text>
          </View>
        )}

        {error && (
          <View style={styles.errorBox}>
            <Text style={styles.errorText}>{error}</Text>
          </View>
        )}

        <TextInput
          style={[
            styles.input,
            darkMode ? styles.darkInput : styles.lightInput,
          ]}
          placeholder="Display name"
          placeholderTextColor={darkMode ? "#888" : "#666"}
          value={username}
          onChangeText={setUsername}
          autoCapitalize="words"
          autoCorrect={false}
        />

        <TouchableOpacity
          style={[styles.primaryButton, !username && styles.disabledButton]}
          onPress={handleSignUp}
          disabled={!username}
        >
          <Text style={styles.primaryButtonText}>Sign Up with Passkey</Text>
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
              darkMode ? styles.darkSubtext : styles.lightSubtext,
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

        <TouchableOpacity style={styles.secondaryButton} onPress={handleLogIn}>
          <Text
            style={[
              styles.secondaryButtonText,
              darkMode ? styles.darkText : styles.lightText,
            ]}
          >
            Log In with Existing Passkey
          </Text>
        </TouchableOpacity>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  darkBg: {
    backgroundColor: "#000",
  },
  lightBg: {
    backgroundColor: "#f5f5f5",
  },
  darkText: {
    color: "#fff",
  },
  lightText: {
    color: "#000",
  },
  darkSubtext: {
    color: "#888",
  },
  lightSubtext: {
    color: "#666",
  },
  authContainer: {
    flex: 1,
    justifyContent: "center",
    padding: 24,
  },
  title: {
    fontSize: 28,
    fontWeight: "bold",
    textAlign: "center",
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 16,
    textAlign: "center",
    marginBottom: 32,
  },
  warningBox: {
    backgroundColor: "#fff3cd",
    padding: 12,
    borderRadius: 8,
    marginBottom: 16,
  },
  warningText: {
    color: "#856404",
    fontSize: 14,
  },
  errorBox: {
    backgroundColor: "#f8d7da",
    padding: 12,
    borderRadius: 8,
    marginBottom: 16,
  },
  errorText: {
    color: "#721c24",
    fontSize: 14,
  },
  input: {
    borderWidth: 1,
    borderRadius: 8,
    padding: 14,
    fontSize: 16,
    marginBottom: 16,
  },
  darkInput: {
    borderColor: "#333",
    backgroundColor: "#1a1a1a",
    color: "#fff",
  },
  lightInput: {
    borderColor: "#ddd",
    backgroundColor: "#fff",
    color: "#000",
  },
  primaryButton: {
    backgroundColor: "#007aff",
    padding: 16,
    borderRadius: 8,
    alignItems: "center",
  },
  primaryButtonText: {
    color: "#fff",
    fontSize: 16,
    fontWeight: "600",
  },
  disabledButton: {
    opacity: 0.5,
  },
  divider: {
    flexDirection: "row",
    alignItems: "center",
    marginVertical: 24,
  },
  dividerLine: {
    flex: 1,
    height: 1,
  },
  darkDivider: {
    backgroundColor: "#333",
  },
  lightDivider: {
    backgroundColor: "#ddd",
  },
  dividerText: {
    marginHorizontal: 16,
    fontSize: 14,
  },
  secondaryButton: {
    padding: 16,
    borderRadius: 8,
    alignItems: "center",
    borderWidth: 1,
    borderColor: "#007aff",
  },
  secondaryButtonText: {
    fontSize: 16,
  },
});
