import { useEffect, useState } from "react";
import { SafeAreaView, ScrollView, StyleSheet, Text, Pressable } from "react-native";
import { encodeResult } from "./src/protocol";
import { scenarioPlan } from "./src/scenarios";
import {
  admittedNativeRelay,
  deviceReceiptContext,
  recordDeviceReceipt,
} from "./src/native-fixture";
import { proveAdmittedRelay } from "./src/relay-admission";

async function observeLinkedAbiAdmission() {
  const { executor, capability } = await admittedNativeRelay();
  await proveAdmittedRelay(executor, capability);
  return await deviceReceiptContext();
}

export default function App() {
  const [shown, setShown] = useState(false);
  const [error, setError] = useState<string>();
  useEffect(() => {
    void (async () => {
      const receipt = await observeLinkedAbiAdmission();
      const linked = scenarioPlan.find((scenario) => scenario.scenario === "linked-abi-admission")!;
      const result = encodeResult({
        ...linked,
        receipt: { ...receipt, sequence: 1, observedAt: new Date().toISOString() },
      });
      // The host validates this independently. Persisting it is necessary on
      // iOS release builds, where console output is not a dependable receipt
      // transport; it must happen after the JS-side relay proof.
      await recordDeviceReceipt(result);
      console.log(result);
      return receipt;
    })()
      .then((receipt) => {
        setShown(true);
      })
      .catch((reason: unknown) => {
        const detail = reason instanceof Error ? reason.message : String(reason);
        console.error("linked-abi-admission failed", detail);
        setError(detail);
      });
  }, []);
  return (
    <SafeAreaView style={styles.page}>
      <ScrollView contentContainerStyle={styles.content}>
        <Text style={styles.title}>Jazz device acceptance</Text>
        <Text style={styles.copy}>
          Development build only. Expo Go cannot contain the native relay.
        </Text>
        <Pressable testID="emit-scenario-plan" style={styles.button} onPress={() => setShown(true)}>
          <Text style={styles.buttonText}>Emit acceptance plan</Text>
        </Pressable>
        {shown &&
          scenarioPlan.map((item) => (
            <Text key={item.scenario} testID={`scenario-${item.scenario}`}>
              {item.scenario}: {item.state} — {item.detail}
            </Text>
          ))}
        {error && <Text testID="scenario-error">linked-abi-admission: failed — {error}</Text>}
      </ScrollView>
    </SafeAreaView>
  );
}
const styles = StyleSheet.create({
  page: { flex: 1 },
  content: { padding: 24, gap: 14 },
  title: { fontSize: 24, fontWeight: "700" },
  copy: { color: "#444" },
  button: { backgroundColor: "#111827", borderRadius: 6, padding: 12 },
  buttonText: { color: "white", fontWeight: "600" },
});
