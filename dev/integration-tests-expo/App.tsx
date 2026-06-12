import * as React from "react";
import { SafeAreaView, StatusBar, StyleSheet } from "react-native";
import { TestRunnerScreen } from "./ui/TestRunnerScreen";
import { suites } from "./tests/_registry";

// No JazzProvider: the runner creates a fresh local `Db` per test via
// `createDb` directly. Runtime polyfills are installed in index.js.
export default function App() {
  return (
    <SafeAreaView style={styles.container}>
      <StatusBar barStyle="dark-content" />
      <TestRunnerScreen suites={suites} />
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: "#f4f4f4" },
});
