import { useEffect, useState } from "react";
import { SafeAreaView, ScrollView, StyleSheet, Text, Pressable } from "react-native";
import { encodeResult } from "./src/protocol";
import { scenarioPlan, scenariosForAcceptancePhase } from "./src/scenarios";
import {
  proveForegroundByteAbi,
  proveForegroundRevoked,
  proveForegroundScopeIsolation,
  proveForegroundWriteAbi,
  proveSameJsiRuntimeWriteSubscription,
} from "./src/foreground-byte-abi";
import {
  proveHighLevelForegroundRestart,
  seedHighLevelForegroundRuntime,
} from "./src/high-level-foreground";
import {
  admittedNativeRelay,
  deviceReceiptContext,
  logoutNativeRelay,
  nativeAcceptancePhase,
  recordDeviceDiagnostic,
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
  // The native fixture returns the same host-issued nonce from both launches.
  // It is also bound into every accepted device receipt, so use it to make
  // retained app data from an old install unable to satisfy this run's reopen
  // assertion.
  const receipt = await deviceReceiptContext();
  const phase = await nativeAcceptancePhase();
  if (phase === "verify") {
    // This is intentionally a new JS and native process. The row was committed
    // through `createJazzClient` by the previous seed launch; this launch must
    // materialize it through a newly admitted relay/SQLite owner using that
    // same public app surface, before the byte-level scope isolation receipt.
    const reopened = await admittedNativeRelay();
    await proveHighLevelForegroundRestart(reopened.capability, receipt.runNonce);
    const foregroundFactory = installNativeForegroundRuntime();
    const foregroundCodec = {
      encode: encodeNativeForegroundCommand,
      decode: decodeNativeForegroundResponse,
    };
    proveForegroundScopeIsolation(foregroundFactory, reopened.capability, foregroundCodec, {
      contains: ["a"],
      excludes: ["b"],
    });
    const scopeB = await switchNativeRelayAuthScope();
    proveForegroundScopeIsolation(foregroundFactory, scopeB.capability, foregroundCodec, {
      contains: ["b"],
      excludes: ["a"],
    });
    await logoutNativeRelay();
    return { phase, receipt };
  }
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
  await seedHighLevelForegroundRuntime(scopeA.capability, receipt.runNonce);
  proveForegroundScopeIsolation(foregroundFactory, scopeA.capability, foregroundCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });
  const oldScopeForeground = foregroundFactory.openAttached(scopeA.capability);
  const scopeB = await proveAuthScopeSwitch(scopeA, switchNativeRelayAuthScope);
  proveForegroundRevoked(oldScopeForeground, foregroundCodec.encode);
  proveForegroundByteAbi(foregroundFactory, scopeB.capability, foregroundCodec);
  proveForegroundScopeIsolation(foregroundFactory, scopeB.capability, foregroundCodec, {
    write: "b",
    contains: ["b"],
    excludes: ["a"],
  });
  // This remains byte-only JSI transport: the fixed test record envelope is
  // decoded by the compiled Rust relay, never reconstructed as a JS row API.
  proveForegroundWriteAbi(foregroundFactory, scopeB.capability, foregroundCodec);
  // Two aliases opened in this one installed JSI runtime communicate only
  // through their common admitted native relay; B must observe A's committed
  // binding delta. This is deliberately not evidence for two physical JSI
  // runtimes; that installed-app receipt remains an explicit gap below.
  proveSameJsiRuntimeWriteSubscription(foregroundFactory, scopeB.capability, foregroundCodec);
  // Closing B's trusted relay before re-admitting A forces its scope owner and
  // SQLite handle to be recreated. A's row must survive that lifecycle while
  // B's distinct native-selected path never observed it.
  await logoutNativeRelay();
  const reopenedScopeA = await admittedNativeRelay();
  proveForegroundScopeIsolation(foregroundFactory, reopenedScopeA.capability, foregroundCodec, {
    contains: ["a"],
    excludes: ["b"],
  });
  await logoutNativeRelay();
  return { phase, receipt };
}

export default function App() {
  const [shown, setShown] = useState(false);
  const [error, setError] = useState<string>();
  useEffect(() => {
    void (async () => {
      const observed = await observeTrustedAdmissionLifecycle();
      const results = scenariosForAcceptancePhase(observed.phase)
        .filter((scenario) => scenario.state === "passed")
        .map((scenario, index) =>
          encodeResult({
            ...scenario,
            receipt: {
              ...observed.receipt,
              sequence: index + 1,
              observedAt: new Date().toISOString(),
            },
          }),
        );
      // The host validates this independently. Persisting it is necessary on
      // iOS release builds, where console output is not a dependable receipt
      // transport; it must happen after the JS-side relay proof.
      await recordDeviceReceipt(results.join("\n"));
      for (const result of results) console.log(result);
      return observed;
    })()
      .then(() => {
        setShown(true);
      })
      .catch(() => {
        // This is deliberately a fixed code, not an exception message: both
        // the app-private file and a failing CI job may expose it. The native
        // fixtures accept only this small allowlist, too.
        void recordDeviceDiagnostic("linked-abi-admission-failed").catch(() => {});
        setError("The device acceptance proof failed.");
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
