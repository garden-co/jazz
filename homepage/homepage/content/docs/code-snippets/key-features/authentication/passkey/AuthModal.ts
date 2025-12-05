import { JazzBrowserContextManager, BrowserPasskeyAuth } from 'jazz-tools/browser';

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
const authSecretStorage = contextManager.getAuthSecretStorage();
const appName = "My Jazz App"

const auth = new BrowserPasskeyAuth(
  crypto,
  authenticate,
  authSecretStorage,
  appName,
);

// To register a new account, use auth.signUp(name: string)
// To log in to an existing account with a passkey, use auth.signIn() 