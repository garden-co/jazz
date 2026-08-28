import { useEffect, useState } from "react";
import { SafeAreaView, ScrollView, StyleSheet, Text, Pressable } from "react-native";
import { encodeResult } from "./src/protocol";
import { scenarioPlan } from "./src/scenarios";
import {
  proveForegroundByteAbi,
  proveForegroundRevoked,
  proveForegroundWriteAbi,
} from "./src/foreground-byte-abi";
import {
  admittedNativeRelay,
  deviceReceiptContext,
  logoutNativeRelay,
  recordDeviceReceipt,
  switchNativeRelayAuthScope,
} from "./src/native-fixture";
import {
  decodeNativeForegroundResponse,
  encodeNativeForegroundCommand,
  installNativeForegroundRuntime,
} from "jazz-rn";
import {
  proveAdmittedRelay,
  proveAuthScopeSwitch,
  proveLogoutRevocation,
} from "./src/relay-admission";

async function observeTrustedAdmissionLifecycle() {
  const admitted = await admittedNativeRelay();
  const { executor, capability } = admitted;
  await proveAdmittedRelay(executor, capability);
  const foregroundFactory = installNativeForegroundRuntime();
  const foregroundCodec = {
    encode: encodeNativeForegroundCommand,
    decode: decodeNativeForegroundResponse,
  };
  proveForegroundByteAbi(foregroundFactory, capability, foregroundCodec);
  const revocableForeground = foregroundFactory.openAttached(capability);
  await proveLogoutRevocation(
    admitted,
    async () => {
      await logoutNativeRelay();
      proveForegroundRevoked(revocableForeground, foregroundCodec.encode);
    },
    admittedNativeRelay,
  );
  const scopeA = await admittedNativeRelay();
  const oldScopeForeground = foregroundFactory.openAttached(scopeA.capability);
  const scopeB = await proveAuthScopeSwitch(scopeA, switchNativeRelayAuthScope);
  proveForegroundRevoked(oldScopeForeground, foregroundCodec.encode);
  proveForegroundByteAbi(foregroundFactory, scopeB.capability, foregroundCodec);
  // This remains byte-only JSI transport: the fixed test record envelope is
  // decoded by the compiled Rust relay, never reconstructed as a JS row API.
  proveForegroundWriteAbi(foregroundFactory, scopeB.capability, foregroundCodec);
  return await deviceReceiptContext();
}

export default function App() {
  const [shown, setShown] = useState(false);
  const [error, setError] = useState<string>();
  useEffect(() => {
    void (async () => {
      const receipt = await observeTrustedAdmissionLifecycle();
      const results = scenarioPlan
        .filter((scenario) => scenario.state === "passed")
        .map((scenario, index) =>
          encodeResult({
            ...scenario,
            receipt: { ...receipt, sequence: index + 1, observedAt: new Date().toISOString() },
          }),
        );
      // The host validates this independently. Persisting it is necessary on
      // iOS release builds, where console output is not a dependable receipt
      // transport; it must happen after the JS-side relay proof.
      await recordDeviceReceipt(results.join("\n"));
      for (const result of results) console.log(result);
      return receipt;
    })()
      .then(() => {
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
