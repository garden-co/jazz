import { useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { authClient } from "./auth-client";

export function SignInForm({ onToggle }: { onToggle: () => void }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);

  async function handleSubmit() {
    setError(null);
    setIsPending(true);
    const result = await authClient.signIn.email({ email, password });
    setIsPending(false);
    if (result.error) {
      setError(result.error.message ?? "Sign-in failed");
    }
  }

  return (
    <View style={styles.card}>
      <Text style={styles.heading}>Sign in</Text>
      <View style={styles.field}>
        <Text style={styles.label}>Email</Text>
        <TextInput
          value={email}
          onChangeText={setEmail}
          style={styles.input}
          autoCapitalize="none"
          autoCorrect={false}
          keyboardType="email-address"
        />
      </View>
      <View style={styles.field}>
        <Text style={styles.label}>Password</Text>
        <TextInput
          value={password}
          onChangeText={setPassword}
          style={styles.input}
          secureTextEntry
        />
      </View>
      {error ? <Text style={styles.error}>{error}</Text> : null}
      <Pressable
        onPress={handleSubmit}
        disabled={isPending}
        style={[styles.primary, isPending && styles.disabled]}
      >
        <Text style={styles.primaryText}>Sign in</Text>
      </Pressable>
      <Pressable onPress={onToggle} style={styles.toggle}>
        <Text style={styles.toggleText}>New here? Create an account</Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: "#fff",
    borderRadius: 12,
    padding: 20,
    borderWidth: 1,
    borderColor: "#e5e7eb",
    gap: 12,
  },
  heading: { fontSize: 22, fontWeight: "700", color: "#111827" },
  field: { gap: 4 },
  label: { color: "#374151", fontSize: 13, fontWeight: "500" },
  input: {
    borderWidth: 1,
    borderColor: "#d1d5db",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  error: { color: "#b91c1c", fontWeight: "600" },
  primary: {
    alignItems: "center",
    paddingVertical: 12,
    borderRadius: 10,
    backgroundColor: "#111827",
  },
  disabled: { opacity: 0.5 },
  primaryText: { color: "#fff", fontWeight: "600" },
  toggle: { alignItems: "center", paddingTop: 4 },
  toggleText: { color: "#2563eb", fontSize: 13 },
});
