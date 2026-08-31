import { useEffect, useState } from "react";
import { SafeAreaView, ScrollView, StyleSheet, Text, Pressable } from "react-native";
import { encodeResult } from "./src/protocol";
import { scenarioPlan, scenariosForAcceptancePhase } from "./src/scenarios";
import type { DeviceDiagnosticCode } from "./src/device-diagnostics";
import {
  proveForegroundByteAbi,
  proveForegroundRevoked,
  proveForegroundScopeIsolation,
  proveForegroundWriteAbi,
  proveSameJsiRuntimeWriteSubscription,
} from "./src/foreground-byte-abi";
import {
  proveHighLevelForegroundRestart,
  proveHighLevelForegroundRelayReadback,
  seedHighLevelForegroundRuntime,
} from "./src/high-level-foreground";
import {
  admittedNativeRelay,
  clearDeviceDiagnostic,
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
import { createDeviceDiagnosticTracker } from "./src/diagnostic-lifecycle";
import { rowIdForRun } from "./src/run-marker";

async function observeTrustedAdmissionLifecycle(markFailure: (code: DeviceDiagnosticCode) => void) {
  // The native fixture returns the same host-issued nonce from both launches.
  // It is also bound into every accepted device receipt, so use it to make
  // retained app data from an old install unable to satisfy this run's reopen
  // assertion.
  markFailure("fixture-metadata-failed");
  const receipt = await deviceReceiptContext();
  const phase = await nativeAcceptancePhase();
  if (phase === "verify") {
    // This is intentionally a new JS and native process. The row was committed
    // through `createJazzClient` by the previous seed launch; this launch must
    // materialize it through a newly admitted relay/SQLite owner using that
    // same public app surface, before the byte-level scope isolation receipt.
    markFailure("native-admission-failed");
    const reopened = await admittedNativeRelay();
    markFailure("public-client-restart-failed");
    await proveHighLevelForegroundRestart(reopened.capability, receipt.runNonce);
    markFailure("foreground-byte-abi-failed");
    const foregroundFactory = installNativeForegroundRuntime();
    const foregroundCodec = {
      encode: encodeNativeForegroundCommand,
      decode: decodeNativeForegroundResponse,
    };
    markFailure("scope-isolation-failed");
    await proveForegroundScopeIsolation(
      foregroundFactory,
      reopened.capability,
      foregroundCodec,
      {
        contains: ["a"],
        excludes: ["b"],
      },
      markFailure,
    );
    markFailure("auth-switch-failed");
    const scopeB = await switchNativeRelayAuthScope();
    markFailure("scope-isolation-failed");
    await proveForegroundScopeIsolation(
      foregroundFactory,
      scopeB.capability,
      foregroundCodec,
      {
        contains: ["b"],
        excludes: ["a"],
      },
      markFailure,
    );
    markFailure("logout-revocation-failed");
    await logoutNativeRelay();
    return { phase, receipt };
  }
  markFailure("native-admission-failed");
  const admitted = await admittedNativeRelay();
  const { executor, capability } = admitted;
  markFailure("relay-command-abi-failed");
  await proveAdmittedRelay(executor, capability, markFailure);
  markFailure("foreground-byte-abi-failed");
  markFailure("foreground-install-failed");
  const foregroundFactory = installNativeForegroundRuntime();
  const foregroundCodec = {
    encode: encodeNativeForegroundCommand,
    decode: decodeNativeForegroundResponse,
  };
  proveForegroundByteAbi(foregroundFactory, capability, foregroundCodec, markFailure);
  markFailure("foreground-open-failed");
  const revocableForeground = foregroundFactory.openAttached(capability);
  markFailure("logout-revocation-failed");
  await proveLogoutRevocation(
    admitted,
    async () => {
      await logoutNativeRelay();
      proveForegroundRevoked(revocableForeground, foregroundCodec.encode);
    },
    admittedNativeRelay,
  );
  markFailure("native-admission-failed");
  const scopeA = await admittedNativeRelay();
  markFailure("public-client-seed-failed");
  await seedHighLevelForegroundRuntime(scopeA.capability, receipt.runNonce, markFailure);
  // The first client is now fully shut down. A new public foreground must
  // read the run-bound row through the persistent relay before the driver
  // terminates the whole app; this keeps the later restart receipt from being
  // the first proof that the seed escaped its in-memory UI preview.
  markFailure("public-client-relay-readback-failed");
  await proveHighLevelForegroundRelayReadback(scopeA.capability, receipt.runNonce);
  markFailure("scope-isolation-failed");
  await proveForegroundScopeIsolation(
    foregroundFactory,
    scopeA.capability,
    foregroundCodec,
    {
      write: "a",
      contains: ["a"],
      excludes: ["b"],
    },
    markFailure,
  );
  const oldScopeForeground = foregroundFactory.openAttached(scopeA.capability);
  markFailure("auth-switch-failed");
  const scopeB = await proveAuthScopeSwitch(scopeA, switchNativeRelayAuthScope);
  markFailure("foreground-byte-abi-failed");
  proveForegroundRevoked(oldScopeForeground, foregroundCodec.encode);
  proveForegroundByteAbi(foregroundFactory, scopeB.capability, foregroundCodec, markFailure);
  markFailure("scope-isolation-failed");
  await proveForegroundScopeIsolation(
    foregroundFactory,
    scopeB.capability,
    foregroundCodec,
    {
      write: "b",
      contains: ["b"],
      excludes: ["a"],
    },
    markFailure,
  );
  // This remains byte-only JSI transport: the fixed test record envelope is
  // decoded by the compiled Rust relay, never reconstructed as a JS row API.
  markFailure("foreground-write-failed");
  proveForegroundWriteAbi(foregroundFactory, scopeB.capability, foregroundCodec);
  // Two aliases opened in this one installed JSI runtime communicate only
  // through their common admitted native relay; B must observe A's committed
  // binding delta. This is deliberately not evidence for two physical JSI
  // runtimes; that installed-app receipt remains an explicit gap below.
  markFailure("same-runtime-subscription-failed");
  await proveSameJsiRuntimeWriteSubscription(
    foregroundFactory,
    scopeB.capability,
    foregroundCodec,
    rowIdForRun(receipt.runNonce),
    markFailure,
  );
  // Closing B's trusted relay before re-admitting A forces its scope owner and
  // SQLite handle to be recreated. A's row must survive that lifecycle while
  // B's distinct native-selected path never observed it.
  markFailure("scope-reopen-failed");
  await logoutNativeRelay();
  const reopenedScopeA = await admittedNativeRelay();
  await proveForegroundScopeIsolation(
    foregroundFactory,
    reopenedScopeA.capability,
    foregroundCodec,
    {
      contains: ["a"],
      excludes: ["b"],
    },
    markFailure,
  );
  markFailure("logout-revocation-failed");
  await logoutNativeRelay();
  return { phase, receipt };
}

export default function App() {
  const [shown, setShown] = useState(false);
  const [error, setError] = useState<string>();
  useEffect(() => {
    const diagnostic = createDeviceDiagnosticTracker(recordDeviceDiagnostic, clearDeviceDiagnostic);
    void (async () => {
      const observed = await observeTrustedAdmissionLifecycle(diagnostic.mark);
      await diagnostic.clear();
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
      diagnostic.mark("receipt-write-failed");
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
        diagnostic.retry();
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
