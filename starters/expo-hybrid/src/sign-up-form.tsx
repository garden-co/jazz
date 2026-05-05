import { useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { useDb } from "jazz-tools/react-native";
import { authClient } from "./auth-client";

export function SignUpForm({ onToggle }: { onToggle: () => void }) {
  const db = useDb();
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);

  async function handleSubmit() {
    setError(null);
    setIsPending(true);

    const proofToken = await db.getLocalFirstIdentityProof({
      ttlSeconds: 60,
      audience: "expo-hybrid-signup",
    });

    if (!proofToken) {
      setError("Sign up requires an active Jazz session");
      setIsPending(false);
      return;
    }

    const { error: signUpError } = await authClient.signUp.email({
      email,
      name,
      password,
      proofToken,
    } as Parameters<typeof authClient.signUp.email>[0]);

    setIsPending(false);
    if (signUpError) {
      setError(signUpError.message ?? "Sign-up failed");
    }
  }

  return (
    <View style={styles.card}>
      <Text style={styles.heading}>Create account</Text>
      <View style={styles.field}>
        <Text style={styles.label}>Name</Text>
        <TextInput
          value={name}
          onChangeText={setName}
          style={styles.input}
          autoCapitalize="words"
          accessibilityLabel="Name"
        />
      </View>
      <View style={styles.field}>
        <Text style={styles.label}>Email</Text>
        <TextInput
          value={email}
          onChangeText={setEmail}
          style={styles.input}
          autoCapitalize="none"
          autoCorrect={false}
          keyboardType="email-address"
          accessibilityLabel="Email"
        />
      </View>
      <View style={styles.field}>
        <Text style={styles.label}>Password</Text>
        <TextInput
          value={password}
          onChangeText={setPassword}
          style={styles.input}
          secureTextEntry
          accessibilityLabel="Password"
          returnKeyType="go"
          onSubmitEditing={handleSubmit}
        />
      </View>
      {error ? <Text style={styles.error}>{error}</Text> : null}
      <Pressable
        onPress={handleSubmit}
        disabled={isPending}
        style={[styles.primary, isPending && styles.disabled]}
        accessibilityLabel="Submit create account"
        accessibilityRole="button"
      >
        <Text style={styles.primaryText}>
          {isPending ? "Creating account..." : "Create account"}
        </Text>
      </Pressable>
      <Pressable onPress={onToggle} style={styles.toggle}>
        <Text style={styles.toggleText}>Already have an account? Sign in</Text>
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
