import { useClerk } from "@clerk/clerk-expo";
import { useAccount } from "jazz-tools/expo";
import { StyleSheet, Text, TouchableOpacity, View } from "react-native";

export function MainScreen() {
  const { me } = useAccount();
  const { signOut } = useClerk();

  const handleSignOut = async () => {
    await signOut();
  };

  return (
    <View style={styles.container}>
      <Text style={styles.title}>You're logged in</Text>
      <Text style={styles.subtitle}>Welcome back, {me?.profile?.name}</Text>
      <TouchableOpacity onPress={handleSignOut}>
        <Text>Sign out</Text>
      </TouchableOpacity>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    padding: 20,
    backgroundColor: "#fff",
  },
  title: {
    fontSize: 24,
    fontWeight: "bold",
    marginBottom: 16,
    textAlign: "center",
  },
  subtitle: {
    fontSize: 16,
    marginBottom: 24,
    textAlign: "center",
    color: "#666",
  },
});
