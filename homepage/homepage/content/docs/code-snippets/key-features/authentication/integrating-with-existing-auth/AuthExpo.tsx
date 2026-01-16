import { useJazzContext } from "jazz-tools/expo";
import { useState } from "react";
import { View, TextInput, Button } from "react-native";

export function Auth() {
  const context = useJazzContext();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  async function handleSubmit() {
    if (!username || !password) {
      throw new Error("Username and password required");
    }

    // Whatever your existing auth system is
    // @ts-expect-error Virtual implementation
    const myOldAppUser = await myApp.logIn(username, password);
    const accountID = myOldAppUser.jazzAccountID;
    // If you've stored this in an encrypted form, make sure to decrypt it first
    const accountSecret = myOldAppUser.jazzAccountSecret;

    await context.authenticate({
      accountID,
      accountSecret,
      provider: "my-old-app-auth", // Use any string here to identify your authentication provider. This avoids Jazz considering your users unauthenticated.
    });

    // The Jazz session is now authenticated!
  }

  return (
    <View style={{ padding: 16 }}>
      <TextInput
        placeholder="Username"
        value={username}
        onChangeText={setUsername}
        autoCapitalize="none"
        style={{ marginBottom: 12, borderWidth: 1, padding: 8 }}
      />

      <TextInput
        placeholder="Password"
        value={password}
        onChangeText={setPassword}
        secureTextEntry
        autoCapitalize="none"
        style={{ marginBottom: 12, borderWidth: 1, padding: 8 }}
      />

      <Button title="Log In" onPress={handleSubmit} />
    </View>
  );
}
