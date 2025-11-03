import { Text, View, StyleSheet } from "react-native";
import { ed25519Sign, newEd25519SigningKey } from "react-native-cojson-core-rn";

const signingKey = newEd25519SigningKey();
const message = new TextEncoder().encode("Hello, World!");
const signature = ed25519Sign(signingKey, message.buffer as ArrayBuffer);
console.log(signature);
export default function App() {
  return (
    <View style={styles.container}>
      <Text>Result: {signature.toString()}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },
});
