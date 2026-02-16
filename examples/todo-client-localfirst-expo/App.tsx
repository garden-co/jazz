import { JazzProvider } from "jazz-tools/react-native";
import { ActivityIndicator, SafeAreaView, StatusBar, StyleSheet, Text, View } from "react-native";
import { TodoList } from "./src/TodoList";

const config = {
  appId: "todo-expo-example",
  env: "dev",
  userBranch: "main",
  // Optional override. If omitted, jazz-rn derives a default SurrealKV path from appId.
  // dataPath: "/absolute/path/to/todo-expo-example.surrealkv",
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#f4f4f4",
  },
  content: {
    flex: 1,
    paddingHorizontal: 16,
    paddingTop: 20,
    gap: 16,
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    color: "#111827",
  },
  loadingContainer: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
  },
  loadingText: {
    color: "#374151",
    fontSize: 14,
  },
});

const fallback = (
  <SafeAreaView style={styles.container}>
    <View style={styles.loadingContainer}>
      <ActivityIndicator size="small" />
      <Text style={styles.loadingText}>Loading Jazz runtime...</Text>
    </View>
  </SafeAreaView>
);

export default function App() {
  return (
    <JazzProvider config={config} fallback={fallback}>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <View style={styles.content}>
          <Text style={styles.title}>Todos</Text>
          <TodoList />
        </View>
      </SafeAreaView>
    </JazzProvider>
  );
}
