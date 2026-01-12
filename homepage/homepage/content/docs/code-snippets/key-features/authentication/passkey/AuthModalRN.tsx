import { View, TextInput, Button, Text } from "react-native";
import { useState } from "react";
import { usePasskeyAuth } from "jazz-tools/react-native";

type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};
// #region Basic
export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [username, setUsername] = useState("");

  const auth = usePasskeyAuth({
    // Must be inside the JazzProvider!
    appName: "My App",
    rpId: "myapp.com", // Your app's domain
  });

  if (auth.state === "signedIn") {
    return <Text>You are already signed in</Text>;
  }

  const handleSignUp = async () => {
    await auth.signUp(username);
    onOpenChange(false);
  };

  const handleLogIn = async () => {
    await auth.logIn();
    onOpenChange(false);
  };

  return (
    <View>
      <Button title="Log in with Passkey" onPress={handleLogIn} />

      <TextInput
        value={username}
        onChangeText={setUsername}
        placeholder="Enter your name"
      />
      <Button title="Sign up with Passkey" onPress={handleSignUp} />
    </View>
  );
}
// #endregion
