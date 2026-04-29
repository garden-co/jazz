import { useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { ExpoAuthSecretStore } from "jazz-tools/expo";

type Status =
  | { kind: "idle" }
  | { kind: "error"; message: string }
  | { kind: "success"; message: string };

export function AuthBackup() {
  const [expanded, setExpanded] = useState(false);
  const [phrase, setPhrase] = useState<string | null>(null);
  const [restoreInput, setRestoreInput] = useState("");
  const [status, setStatus] = useState<Status>({ kind: "idle" });
  const [busy, setBusy] = useState(false);

  async function handleReveal() {
    setStatus({ kind: "idle" });
    setBusy(true);
    try {
      const secret = await ExpoAuthSecretStore.loadSecret();
      if (!secret) {
        setStatus({ kind: "error", message: "No local secret to reveal yet." });
        return;
      }
      const { RecoveryPhrase } = await import("jazz-tools/passphrase");
      setPhrase(RecoveryPhrase.fromSecret(secret));
    } catch (err) {
      setStatus({ kind: "error", message: describeError(err) });
    } finally {
      setBusy(false);
    }
  }

  async function handleRestore() {
    setStatus({ kind: "idle" });
    setBusy(true);
    try {
      const { RecoveryPhrase } = await import("jazz-tools/passphrase");
      const secret = RecoveryPhrase.toSecret(restoreInput.trim());
      await ExpoAuthSecretStore.saveSecret(secret);
      setStatus({ kind: "success", message: "Restored. Restart the app to apply." });
      setRestoreInput("");
    } catch (err) {
      setStatus({ kind: "error", message: describeError(err) });
    } finally {
      setBusy(false);
    }
  }

  return (
    <View style={styles.section}>
      <Pressable onPress={() => setExpanded((v) => !v)}>
        <Text style={styles.summary}>
          {expanded ? "▾" : "▸"} Back up or restore your local-only account
        </Text>
      </Pressable>
      {expanded ? (
        <View style={styles.body}>
          <Text style={styles.hint}>
            Save your account's recovery phrase so you can get back in on another device or after
            clearing storage.
          </Text>

          <View style={styles.subsection}>
            <Text style={styles.subheading}>Recovery phrase</Text>
            <Pressable
              onPress={handleReveal}
              disabled={busy}
              style={[styles.button, busy && styles.buttonDisabled]}
            >
              <Text style={styles.buttonText}>Show recovery phrase</Text>
            </Pressable>
            {phrase ? (
              <TextInput
                style={styles.phrase}
                value={phrase}
                editable={false}
                multiline
                numberOfLines={3}
                selectTextOnFocus
                accessibilityLabel="Recovery phrase"
              />
            ) : null}
          </View>

          <View style={styles.subsection}>
            <Text style={styles.subheading}>Restore from recovery phrase</Text>
            <TextInput
              style={styles.restoreInput}
              value={restoreInput}
              onChangeText={setRestoreInput}
              placeholder="Paste your 24-word phrase"
              multiline
              numberOfLines={3}
              autoCapitalize="none"
              autoCorrect={false}
            />
            <Pressable
              onPress={handleRestore}
              disabled={busy || !restoreInput.trim()}
              style={[styles.button, (busy || !restoreInput.trim()) && styles.buttonDisabled]}
            >
              <Text style={styles.buttonText}>Restore</Text>
            </Pressable>
          </View>

          {status.kind === "error" ? <Text style={styles.error}>{status.message}</Text> : null}
          {status.kind === "success" ? <Text style={styles.success}>{status.message}</Text> : null}
        </View>
      ) : null}
    </View>
  );
}

function describeError(err: unknown): string {
  if (err && typeof err === "object" && "code" in err && "message" in err) {
    const code = String((err as { code: unknown }).code);
    const message = String((err as { message: unknown }).message);
    return `${code}: ${message}`;
  }
  if (err instanceof Error) return err.message;
  return "Unknown error";
}

const styles = StyleSheet.create({
  section: {
    backgroundColor: "#fff",
    borderRadius: 12,
    padding: 16,
    borderWidth: 1,
    borderColor: "#e5e7eb",
    gap: 8,
  },
  summary: {
    fontSize: 16,
    fontWeight: "600",
    color: "#111827",
  },
  body: {
    gap: 12,
    marginTop: 8,
  },
  hint: {
    color: "#4b5563",
    fontSize: 13,
  },
  subsection: {
    gap: 8,
  },
  subheading: {
    fontSize: 14,
    fontWeight: "600",
    color: "#111827",
  },
  button: {
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderRadius: 10,
    backgroundColor: "#111827",
  },
  buttonDisabled: {
    opacity: 0.5,
  },
  buttonText: {
    color: "#fff",
    fontWeight: "600",
  },
  phrase: {
    borderWidth: 1,
    borderColor: "#d1d5db",
    borderRadius: 10,
    padding: 10,
    fontFamily: "Courier",
    color: "#111827",
    backgroundColor: "#f9fafb",
    minHeight: 64,
    textAlignVertical: "top",
  },
  restoreInput: {
    borderWidth: 1,
    borderColor: "#d1d5db",
    borderRadius: 10,
    padding: 10,
    minHeight: 64,
    textAlignVertical: "top",
    backgroundColor: "#fff",
  },
  error: {
    color: "#b91c1c",
    fontWeight: "600",
  },
  success: {
    color: "#047857",
    fontWeight: "600",
  },
});
