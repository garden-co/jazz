<script lang="ts">
  import { LocalFirstAuth } from "jazz-tools/svelte";
  import { goto } from "$app/navigation";

  // In production, pin this to your deployed hostname so passkeys remain
  // usable across preview deployments. Leaving it undefined falls back to
  // location.hostname.
  const PASSKEY_APP_HOSTNAME: string | undefined = undefined;
  const PASSKEY_APP_NAME = "Jazz Starter";

  type Status =
    | { kind: "idle" }
    | { kind: "error"; message: string }
    | { kind: "success"; message: string };

  let {
    redirectAfterRestore,
    mode = "full",
  }: {
    redirectAfterRestore?: string;
    mode?: "full" | "restore-only";
  } = $props();

  const auth = new LocalFirstAuth();

  let phrase = $state<string | null>(null);
  let restoreInput = $state("");
  let status = $state<Status>({ kind: "idle" });
  let busy = $state(false);

  async function navigate() {
    if (redirectAfterRestore) await goto(redirectAfterRestore);
  }

  function describeError(err: unknown): string {
    if (err && typeof err === "object" && "code" in err && "message" in err) {
      return `${String((err as { code: unknown }).code)}: ${String((err as { message: unknown }).message)}`;
    }
    if (err instanceof Error) return err.message;
    return "Unknown error";
  }

  async function handleReveal() {
    status = { kind: "idle" };
    busy = true;
    try {
      if (!auth.secret) {
        status = { kind: "error", message: "No local secret to reveal yet." };
        return;
      }
      const { RecoveryPhrase } = await import("jazz-tools/passphrase");
      phrase = RecoveryPhrase.fromSecret(auth.secret);
    } catch (err) {
      status = { kind: "error", message: describeError(err) };
    } finally {
      busy = false;
    }
  }

  async function handleCopy() {
    if (!phrase) return;
    try {
      await navigator.clipboard.writeText(phrase);
      status = { kind: "success", message: "Phrase copied." };
    } catch {
      status = {
        kind: "error",
        message: "Copy failed — select the text and copy manually.",
      };
    }
  }

  async function handleRestorePhrase(e: SubmitEvent) {
    e.preventDefault();
    status = { kind: "idle" };
    busy = true;
    try {
      const { RecoveryPhrase } = await import("jazz-tools/passphrase");
      const secret = RecoveryPhrase.toSecret(restoreInput.trim());
      await auth.login(secret);
      await navigate();
    } catch (err) {
      status = { kind: "error", message: describeError(err) };
    } finally {
      busy = false;
    }
  }

  async function handlePasskeyBackup() {
    status = { kind: "idle" };
    busy = true;
    try {
      if (!auth.secret) {
        status = { kind: "error", message: "No local secret to back up yet." };
        return;
      }
      const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
      const pb = new BrowserPasskeyBackup({
        appName: PASSKEY_APP_NAME,
        appHostname: PASSKEY_APP_HOSTNAME,
      });
      await pb.backup(auth.secret, "My account");
      status = { kind: "success", message: "Passkey backup created." };
    } catch (err) {
      status = { kind: "error", message: describeError(err) };
    } finally {
      busy = false;
    }
  }

  async function handlePasskeyRestore() {
    status = { kind: "idle" };
    busy = true;
    try {
      const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
      const pb = new BrowserPasskeyBackup({
        appName: PASSKEY_APP_NAME,
        appHostname: PASSKEY_APP_HOSTNAME,
      });
      const secret = await pb.restore();
      await auth.login(secret);
      await navigate();
    } catch (err) {
      status = { kind: "error", message: describeError(err) };
    } finally {
      busy = false;
    }
  }
</script>

<details class="auth-backup">
  <summary>Back up or restore your local-only account</summary>
  <p class="auth-backup-hint">
    Save your account's recovery phrase or a passkey so you can get back in
    on another device or after clearing storage.
  </p>

  <div class="auth-backup-section">
    <h3>Recovery phrase</h3>
    {#if mode === "full"}
      <button type="button" onclick={handleReveal} disabled={busy}>
        Show recovery phrase
      </button>
      {#if phrase}
        <textarea
          class="auth-backup-phrase"
          readonly
          rows={3}
          value={phrase}
          aria-label="Recovery phrase"
        ></textarea>
        <button type="button" onclick={handleCopy} disabled={busy}>
          Copy
        </button>
      {/if}
    {/if}

    <form onsubmit={handleRestorePhrase} class="auth-backup-restore">
      <label for="auth-backup-restore-input">
        Restore from recovery phrase
      </label>
      <textarea
        id="auth-backup-restore-input"
        rows={3}
        bind:value={restoreInput}
        placeholder="Paste your 24-word phrase"
        required
      ></textarea>
      <button type="submit" disabled={busy || !restoreInput.trim()}>
        Restore
      </button>
    </form>
  </div>

  <div class="auth-backup-section">
    <h3>Passkey</h3>
    <div class="auth-backup-row">
      {#if mode === "full"}
        <button type="button" onclick={handlePasskeyBackup} disabled={busy}>
          Back up with passkey
        </button>
      {/if}
      <button type="button" onclick={handlePasskeyRestore} disabled={busy}>
        Restore with passkey
      </button>
    </div>
  </div>

  {#if status.kind === "error"}
    <p class="alert-error" role="alert">{status.message}</p>
  {:else if status.kind === "success"}
    <p class="auth-backup-success" role="status">{status.message}</p>
  {/if}
</details>
