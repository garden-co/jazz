"use client";

import { useState } from "react";
import { BrowserAuthSecretStore } from "jazz-tools";

type Status =
  | { kind: "idle" }
  | { kind: "error"; message: string }
  | { kind: "success"; message: string };

// In production, pin this to your deployed hostname so passkeys remain usable
// across preview deployments. Leaving it undefined falls back to location.hostname.
const PASSKEY_APP_HOSTNAME: string | undefined = undefined;
const PASSKEY_APP_NAME = "Jazz Starter";

export function AuthBackup({
  redirectAfterRestore,
  mode = "full",
}: {
  redirectAfterRestore?: string;
  mode?: "full" | "restore-only";
} = {}) {
  function navigate() {
    if (redirectAfterRestore) {
      location.assign(redirectAfterRestore);
    } else {
      location.reload();
    }
  }
  const [phrase, setPhrase] = useState<string | null>(null);
  const [restoreInput, setRestoreInput] = useState("");
  const [status, setStatus] = useState<Status>({ kind: "idle" });
  const [busy, setBusy] = useState(false);

  async function handleReveal() {
    setStatus({ kind: "idle" });
    setBusy(true);
    try {
      const secret = await BrowserAuthSecretStore.loadSecret();
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

  async function handleCopy() {
    if (!phrase) return;
    try {
      await navigator.clipboard.writeText(phrase);
      setStatus({ kind: "success", message: "Phrase copied." });
    } catch {
      setStatus({
        kind: "error",
        message: "Copy failed — select the text and copy manually.",
      });
    }
  }

  async function handleRestorePhrase(e: React.FormEvent) {
    e.preventDefault();
    setStatus({ kind: "idle" });
    setBusy(true);
    try {
      const { RecoveryPhrase } = await import("jazz-tools/passphrase");
      const secret = RecoveryPhrase.toSecret(restoreInput.trim());
      await BrowserAuthSecretStore.saveSecret(secret);
      navigate();
    } catch (err) {
      setStatus({ kind: "error", message: describeError(err) });
      setBusy(false);
    }
  }

  async function handlePasskeyBackup() {
    setStatus({ kind: "idle" });
    setBusy(true);
    try {
      const secret = await BrowserAuthSecretStore.loadSecret();
      if (!secret) {
        setStatus({ kind: "error", message: "No local secret to back up yet." });
        return;
      }
      const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
      const pb = new BrowserPasskeyBackup({
        appName: PASSKEY_APP_NAME,
        appHostname: PASSKEY_APP_HOSTNAME,
      });
      await pb.backup(secret, "My account");
      setStatus({ kind: "success", message: "Passkey backup created." });
    } catch (err) {
      setStatus({ kind: "error", message: describeError(err) });
    } finally {
      setBusy(false);
    }
  }

  async function handlePasskeyRestore() {
    setStatus({ kind: "idle" });
    setBusy(true);
    try {
      const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
      const pb = new BrowserPasskeyBackup({
        appName: PASSKEY_APP_NAME,
        appHostname: PASSKEY_APP_HOSTNAME,
      });
      const secret = await pb.restore();
      await BrowserAuthSecretStore.saveSecret(secret);
      navigate();
    } catch (err) {
      setStatus({ kind: "error", message: describeError(err) });
      setBusy(false);
    }
  }

  return (
    <details className="auth-backup">
      <summary>Back up or restore your local-only account</summary>
      <p className="auth-backup-hint">
        Save your account's recovery phrase or a passkey so you can get back in on another device or
        after clearing storage.
      </p>

      <div className="auth-backup-section">
        <h3>Recovery phrase</h3>
        {mode === "full" && (
          <>
            <button type="button" onClick={handleReveal} disabled={busy}>
              Show recovery phrase
            </button>
            {phrase && (
              <>
                <textarea
                  className="auth-backup-phrase"
                  readOnly
                  rows={3}
                  value={phrase}
                  aria-label="Recovery phrase"
                />
                <button type="button" onClick={handleCopy} disabled={busy}>
                  Copy
                </button>
              </>
            )}
          </>
        )}

        <form onSubmit={handleRestorePhrase} className="auth-backup-restore">
          <label htmlFor="auth-backup-restore-input">Restore from recovery phrase</label>
          <textarea
            id="auth-backup-restore-input"
            rows={3}
            value={restoreInput}
            onChange={(e) => setRestoreInput(e.target.value)}
            placeholder="Paste your 24-word phrase"
            required
          />
          <button type="submit" disabled={busy || !restoreInput.trim()}>
            Restore
          </button>
        </form>
      </div>

      <div className="auth-backup-section">
        <h3>Passkey</h3>
        <div className="auth-backup-row">
          {mode === "full" && (
            <button type="button" onClick={handlePasskeyBackup} disabled={busy}>
              Back up with passkey
            </button>
          )}
          <button type="button" onClick={handlePasskeyRestore} disabled={busy}>
            Restore with passkey
          </button>
        </div>
      </div>

      {status.kind === "error" && (
        <p className="alert-error" role="alert">
          {status.message}
        </p>
      )}
      {status.kind === "success" && (
        <p className="auth-backup-success" role="status">
          {status.message}
        </p>
      )}
    </details>
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
