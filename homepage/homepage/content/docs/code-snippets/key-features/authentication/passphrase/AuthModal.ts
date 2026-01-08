// #region PassphraseAuth
import { PassphraseAuth } from "jazz-tools";
import { JazzBrowserContextManager } from 'jazz-tools/browser';
import { wordlist } from "./wordlist";

// @ts-expect-error Not a real Vite app
const apiKey = import.meta.env.VITE_JAZZ_API_KEY;
const contextManager = new JazzBrowserContextManager();
await contextManager.createContext({
  sync: {
    peer: `wss://cloud.jazz.tools?key=${apiKey}`
  },
});
const ctx = contextManager.getCurrentValue();
if (!ctx) throw new Error("Context is not available");
const crypto = ctx.node.crypto;
const authenticate = ctx.authenticate;
const register = ctx.register;
const authSecretStorage = contextManager.getAuthSecretStorage();

const auth = new PassphraseAuth(
  crypto,
  authenticate,
  register,
  authSecretStorage,
  wordlist,
);

// Use auth.getCurrentAccountPassphrase() to display the passphrase for the current account
// Use auth.logIn(passphrase) to log in with an existing passphrase
// #endregion
