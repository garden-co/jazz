import { View, TextInput, Button, Text } from "react-native";
import { useState } from "react";
import { usePassphraseAuth } from "jazz-tools/react-native";

type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};
// #region Basic
import { wordlist } from "./wordlist";

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [loginPassphrase, setLoginPassphrase] = useState("");

  const auth = usePassphraseAuth({
    // Must be inside the JazzProvider!
    wordlist: wordlist,
  });

  if (auth.state === "signedIn") {
    return <Text>You are already signed in</Text>;
  }

  const handleSignUp = async () => {
    await auth.signUp();
    onOpenChange(false);
  };

  const handleLogIn = async () => {
    await auth.logIn(loginPassphrase);
    onOpenChange(false);
  };

  return (
    <View>
      <View>
        <Text>Your current passphrase</Text>
        <TextInput
          editable={false}
          value={auth.passphrase}
          multiline
          numberOfLines={5}
        />
      </View>

      <Button title="I have stored my passphrase" onPress={handleSignUp} />

      <View>
        <Text>Log in with your passphrase</Text>
        <TextInput
          value={loginPassphrase}
          onChangeText={setLoginPassphrase}
          placeholder="Enter your passphrase"
          multiline
          numberOfLines={5}
        />
      </View>

      <Button title="Log in" onPress={handleLogIn} />
    </View>
  );
}
// #endregion
