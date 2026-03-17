/**
 * Minimal reproduction: useAccount infinite render loop on Expo New Architecture
 *
 * Bug: calling useAccount({ resolve: {} }) in guest/pre-signup state causes
 * "Maximum update depth exceeded" on New Architecture (newArchEnabled: true).
 *
 * Does NOT reproduce on:
 *   - Old Architecture (newArchEnabled: false in app.json)
 *   - Expo Web target
 *
 * Environment:
 *   - jazz-tools: latest
 *   - Expo SDK 53, New Architecture
 *   - sync.when: "signedUp" (account not yet created)
 */

import { useRef } from "react";
import { Text, View } from "react-native";
import { Account } from "jazz-tools";
import { JazzExpoProvider, useAccount } from "jazz-tools/expo";

function BugTrigger() {
  const renderCount = useRef(0);
  renderCount.current++;

  // This single call is enough to trigger the infinite loop.
  // Removing it stops the loop entirely.
  const me = useAccount(Account, { resolve: {} });

  console.log(`render #${renderCount.current}`, me.$jazz.id);

  return (
    <View style={{ flex: 1, justifyContent: "center", alignItems: "center" }}>
      <Text>Render count: {renderCount.current}</Text>
      <Text>Account: {me.$jazz.id ?? "undefined (guest)"}</Text>
    </View>
  );
}

export default function App() {
  return (
    <JazzExpoProvider
      guestMode
      sync={{
        peer: "wss://cloud.jazz.tools/?key=repro-useaccount-loop@garden.co",
        when: "signedUp",
      }}
    >
      <BugTrigger />
    </JazzExpoProvider>
  );
}
