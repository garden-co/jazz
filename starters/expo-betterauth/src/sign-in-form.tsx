import { useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { authClient } from "./auth-client";

export function SignInForm() {
  const [mode, setMode] = useState<"signin" | "signup">("signin");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);

  async function handleSubmit() {
    setError(null);
    setIsPending(true);

    const result =
      mode === "signup"
        ? await authClient.signUp.email({ name, email, password })
        : await authClient.signIn.email({ email, password });

    setIsPending(false);

    if (result.error) {
      setError(result.error.message ?? (mode === "signup" ? "Sign-up failed" : "Sign-in failed"));
    }
  }

  return (
    <View style={styles.card}>
      <Text style={styles.heading}>{mode === "signup" ? "Create account" : "Sign in"}</Text>
      {mode === "signup" ? (
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
      ) : null}
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
        />
      </View>
      {error ? <Text style={styles.error}>{error}</Text> : null}
      <Pressable
        onPress={handleSubmit}
        disabled={isPending}
        style={[styles.primary, isPending && styles.disabled]}
        accessibilityLabel={mode === "signup" ? "Submit create account" : "Submit sign in"}
        accessibilityRole="button"
      >
        <Text style={styles.primaryText}>{mode === "signup" ? "Create account" : "Sign in"}</Text>
      </Pressable>
      <Pressable
        onPress={() => {
          setMode(mode === "signup" ? "signin" : "signup");
          setError(null);
        }}
        style={styles.toggle}
      >
        <Text style={styles.toggleText}>
          {mode === "signup" ? "Already have an account? Sign in" : "New here? Create an account"}
        </Text>
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
  heading: {
    fontSize: 22,
    fontWeight: "700",
    color: "#111827",
  },
  field: {
    gap: 4,
  },
  label: {
    color: "#374151",
    fontSize: 13,
    fontWeight: "500",
  },
  input: {
    borderWidth: 1,
    borderColor: "#d1d5db",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  error: {
    color: "#b91c1c",
    fontWeight: "600",
  },
  primary: {
    alignItems: "center",
    paddingVertical: 12,
    borderRadius: 10,
    backgroundColor: "#111827",
  },
  disabled: {
    opacity: 0.5,
  },
  primaryText: {
    color: "#fff",
    fontWeight: "600",
  },
  toggle: {
    alignItems: "center",
    paddingTop: 4,
  },
  toggleText: {
    color: "#2563eb",
    fontSize: 13,
  },
});
