<template>
  <details class="auth-backup">
    <summary>Back up or restore your local-only account</summary>
    <p class="auth-backup-hint">
      Save your account's recovery phrase or a passkey so you can get back in on another device or
      after clearing storage.
    </p>

    <div class="auth-backup-section">
      <h3>Recovery phrase</h3>
      <button type="button" @click="handleReveal" :disabled="busy">Show recovery phrase</button>
      <template v-if="phrase">
        <textarea
          class="auth-backup-phrase"
          readonly
          :rows="3"
          :value="phrase"
          aria-label="Recovery phrase"
        />
        <button type="button" @click="handleCopy" :disabled="busy">Copy</button>
      </template>

      <form @submit.prevent="handleRestorePhrase" class="auth-backup-restore">
        <label for="auth-backup-restore-input">Restore from recovery phrase</label>
        <textarea
          id="auth-backup-restore-input"
          :rows="3"
          v-model="restoreInput"
          placeholder="Paste your 24-word phrase"
          required
        />
        <button type="submit" :disabled="busy || !restoreInput.trim()">Restore</button>
      </form>
    </div>

    <div class="auth-backup-section">
      <h3>Passkey</h3>
      <div class="auth-backup-row">
        <button type="button" @click="handlePasskeyBackup" :disabled="busy">
          Back up with passkey
        </button>
        <button type="button" @click="handlePasskeyRestore" :disabled="busy">
          Restore with passkey
        </button>
      </div>
    </div>

    <p v-if="status.kind === 'error'" class="alert-error" role="alert">{{ status.message }}</p>
    <p v-if="status.kind === 'success'" class="auth-backup-success" role="status">
      {{ status.message }}
    </p>
  </details>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { BrowserAuthSecretStore } from "jazz-tools";

type Status =
  | { kind: "idle" }
  | { kind: "error"; message: string }
  | { kind: "success"; message: string };

const PASSKEY_APP_NAME = "Jazz Starter";
const PASSKEY_APP_HOSTNAME: string | undefined = undefined;

const phrase = ref<string | null>(null);
const restoreInput = ref("");
const status = ref<Status>({ kind: "idle" });
const busy = ref(false);

async function handleReveal() {
  status.value = { kind: "idle" };
  busy.value = true;
  try {
    const secret = await BrowserAuthSecretStore.loadSecret();
    if (!secret) {
      status.value = { kind: "error", message: "No local secret to reveal yet." };
      return;
    }
    const { RecoveryPhrase } = await import("jazz-tools/passphrase");
    phrase.value = RecoveryPhrase.fromSecret(secret);
  } catch (err) {
    status.value = { kind: "error", message: describeError(err) };
  } finally {
    busy.value = false;
  }
}

async function handleCopy() {
  if (!phrase.value) return;
  try {
    await navigator.clipboard.writeText(phrase.value);
    status.value = { kind: "success", message: "Phrase copied." };
  } catch {
    status.value = { kind: "error", message: "Copy failed — select the text and copy manually." };
  }
}

async function handleRestorePhrase() {
  status.value = { kind: "idle" };
  busy.value = true;
  try {
    const { RecoveryPhrase } = await import("jazz-tools/passphrase");
    const secret = RecoveryPhrase.toSecret(restoreInput.value.trim());
    await BrowserAuthSecretStore.saveSecret(secret);
    location.reload();
  } catch (err) {
    status.value = { kind: "error", message: describeError(err) };
    busy.value = false;
  }
}

async function handlePasskeyBackup() {
  status.value = { kind: "idle" };
  busy.value = true;
  try {
    const secret = await BrowserAuthSecretStore.loadSecret();
    if (!secret) {
      status.value = { kind: "error", message: "No local secret to back up yet." };
      return;
    }
    const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
    const pb = new BrowserPasskeyBackup({
      appName: PASSKEY_APP_NAME,
      appHostname: PASSKEY_APP_HOSTNAME,
    });
    await pb.backup(secret, "My account");
    status.value = { kind: "success", message: "Passkey backup created." };
  } catch (err) {
    status.value = { kind: "error", message: describeError(err) };
  } finally {
    busy.value = false;
  }
}

async function handlePasskeyRestore() {
  status.value = { kind: "idle" };
  busy.value = true;
  try {
    const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
    const pb = new BrowserPasskeyBackup({
      appName: PASSKEY_APP_NAME,
      appHostname: PASSKEY_APP_HOSTNAME,
    });
    const secret = await pb.restore();
    await BrowserAuthSecretStore.saveSecret(secret);
    location.reload();
  } catch (err) {
    status.value = { kind: "error", message: describeError(err) };
    busy.value = false;
  }
}

function describeError(err: unknown): string {
  if (err && typeof err === "object" && "code" in err && "message" in err) {
    return `${String((err as { code: unknown }).code)}: ${String((err as { message: unknown }).message)}`;
  }
  if (err instanceof Error) return err.message;
  return "Unknown error";
}
</script>
