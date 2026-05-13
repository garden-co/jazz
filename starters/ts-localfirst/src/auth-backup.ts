import { BrowserAuthSecretStore } from "jazz-tools";

// In production, pin this to your deployed hostname so passkeys remain usable
// across preview deployments. Leaving it undefined falls back to location.hostname.
const PASSKEY_APP_HOSTNAME: string | undefined = undefined;
const PASSKEY_APP_NAME = "Jazz Starter";

export interface AuthBackupOptions {
  redirectAfterRestore?: string;
  mode?: "full" | "restore-only";
}

export function mountAuthBackup(parent: HTMLElement, options: AuthBackupOptions = {}): void {
  const { redirectAfterRestore, mode = "full" } = options;
  parent.innerHTML = `
    <details class="auth-backup">
      <summary>Back up or restore your local-only account</summary>
      <p class="auth-backup-hint">
        Save your account's recovery phrase or a passkey so you can get back in on another device or
        after clearing storage.
      </p>

      <div class="auth-backup-section">
        <h3>Recovery phrase</h3>
        ${
          mode === "full"
            ? `<button type="button" data-action="reveal">Show recovery phrase</button>
               <div data-slot="phrase" hidden>
                 <textarea class="auth-backup-phrase" readonly rows="3" aria-label="Recovery phrase"></textarea>
                 <button type="button" data-action="copy">Copy</button>
               </div>`
            : ""
        }
        <form class="auth-backup-restore" data-action="restore">
          <label for="auth-backup-restore-input">Restore from recovery phrase</label>
          <textarea id="auth-backup-restore-input" rows="3" placeholder="Paste your 24-word phrase" required></textarea>
          <button type="submit" disabled>Restore</button>
        </form>
      </div>

      <div class="auth-backup-section">
        <h3>Passkey</h3>
        <div class="auth-backup-row">
          ${
            mode === "full"
              ? `<button type="button" data-action="passkey-backup">Back up with passkey</button>`
              : ""
          }
          <button type="button" data-action="passkey-restore">Restore with passkey</button>
        </div>
      </div>

      <p data-slot="status" hidden></p>
    </details>
  `;

  const statusEl = parent.querySelector<HTMLParagraphElement>('[data-slot="status"]')!;
  const phraseSlot = parent.querySelector<HTMLDivElement>('[data-slot="phrase"]');
  const phraseInput = phraseSlot?.querySelector<HTMLTextAreaElement>("textarea") ?? null;
  const restoreForm = parent.querySelector<HTMLFormElement>('form[data-action="restore"]')!;
  const restoreInput = restoreForm.querySelector<HTMLTextAreaElement>("textarea")!;
  const restoreButton = restoreForm.querySelector<HTMLButtonElement>("button[type='submit']")!;

  function navigate() {
    if (redirectAfterRestore) {
      location.assign(redirectAfterRestore);
    } else {
      location.reload();
    }
  }

  function setStatus(kind: "idle" | "error" | "success", message = "") {
    if (kind === "idle" || !message) {
      statusEl.hidden = true;
      statusEl.textContent = "";
      statusEl.className = "";
      statusEl.removeAttribute("role");
      return;
    }
    statusEl.hidden = false;
    statusEl.textContent = message;
    if (kind === "error") {
      statusEl.className = "alert-error";
      statusEl.setAttribute("role", "alert");
    } else {
      statusEl.className = "auth-backup-success";
      statusEl.setAttribute("role", "status");
    }
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

  async function withBusy<T>(fn: () => Promise<T>): Promise<T | undefined> {
    setBusy(true);
    setStatus("idle");
    try {
      return await fn();
    } catch (err) {
      setStatus("error", describeError(err));
      return undefined;
    } finally {
      setBusy(false);
    }
  }

  function setBusy(busy: boolean) {
    for (const btn of parent.querySelectorAll<HTMLButtonElement>("button")) {
      if (btn.dataset.alwaysEnabled === "true") continue;
      btn.disabled = busy || (btn.type === "submit" && !restoreInput.value.trim());
    }
  }

  restoreInput.addEventListener("input", () => {
    restoreButton.disabled = !restoreInput.value.trim();
  });

  parent.addEventListener("click", async (event) => {
    const target = event.target as HTMLElement;
    const action = target.dataset.action;
    if (!action) return;

    if (action === "reveal" && phraseInput && phraseSlot) {
      await withBusy(async () => {
        const secret = await BrowserAuthSecretStore.loadSecret();
        if (!secret) {
          setStatus("error", "No local secret to reveal yet.");
          return;
        }
        const { RecoveryPhrase } = await import("jazz-tools/passphrase");
        phraseInput.value = RecoveryPhrase.fromSecret(secret);
        phraseSlot.hidden = false;
      });
      return;
    }

    if (action === "copy" && phraseInput) {
      try {
        await navigator.clipboard.writeText(phraseInput.value);
        setStatus("success", "Phrase copied.");
      } catch {
        setStatus("error", "Copy failed — select the text and copy manually.");
      }
      return;
    }

    if (action === "passkey-backup") {
      await withBusy(async () => {
        const secret = await BrowserAuthSecretStore.loadSecret();
        if (!secret) {
          setStatus("error", "No local secret to back up yet.");
          return;
        }
        const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
        const pb = new BrowserPasskeyBackup({
          appName: PASSKEY_APP_NAME,
          appHostname: PASSKEY_APP_HOSTNAME,
        });
        await pb.backup(secret, "My account");
        setStatus("success", "Passkey backup created.");
      });
      return;
    }

    if (action === "passkey-restore") {
      await withBusy(async () => {
        const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
        const pb = new BrowserPasskeyBackup({
          appName: PASSKEY_APP_NAME,
          appHostname: PASSKEY_APP_HOSTNAME,
        });
        const secret = await pb.restore();
        await BrowserAuthSecretStore.saveSecret(secret);
        navigate();
      });
    }
  });

  restoreForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(async () => {
      const { RecoveryPhrase } = await import("jazz-tools/passphrase");
      const secret = RecoveryPhrase.toSecret(restoreInput.value.trim());
      await BrowserAuthSecretStore.saveSecret(secret);
      navigate();
    });
  });
}
