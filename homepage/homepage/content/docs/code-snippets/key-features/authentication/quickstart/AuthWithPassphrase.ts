// [!code ++:2]
import { PassphraseAuth } from "jazz-tools";
import { wordlist } from "./wordlist";
import {
  BrowserPasskeyAuth,
  JazzBrowserContextManager,
} from "jazz-tools/browser";
import { JazzFestAccount } from "./schema";

export const AuthComponent = (
  contextManager: JazzBrowserContextManager<typeof JazzFestAccount>
) => {
  const ctx = contextManager.getCurrentValue();
  if (!ctx) throw new Error("Context is not available");
  const crypto = ctx.node.crypto;
  const authenticate = ctx.authenticate;
  const register = ctx.register;
  const authSecretStorage = contextManager.getAuthSecretStorage();
  const appName = "JazzFest";

  const auth = new BrowserPasskeyAuth(crypto, authenticate, authSecretStorage, appName);

  // [!code ++:1]
  const passphraseAuth = new PassphraseAuth(crypto, authenticate, register, authSecretStorage, wordlist)

  const signUpForm = document.createElement("form");
  const nameInput = Object.assign(document.createElement("input"), {
    placeholder: "Name",
    required: true,
  });
  const signInButton = Object.assign(document.createElement("button"), {
    type: "button",
    innerText: "Sign In",
    onclick: async (evt: MouseEvent) => {
      evt.preventDefault();
      await auth.logIn();
      window.location.href = "/";
    },
  });
  const signUpButton = Object.assign(document.createElement("button"), {
    type: "submit",
    innerText: "Sign Up",
    onclick: async (evt: MouseEvent) => {
      evt.preventDefault();
      await auth.signUp(nameInput.value);
      window.location.href = "/";
    },
  });

  // [!code ++:4]
  const passphraseDisplay = Object.assign(document.createElement("textarea"), {
    rows: 5,
  });

  // [!code ++:3]
  passphraseAuth.getCurrentAccountPassphrase().then((passphrase) => {
    passphraseDisplay.value = passphrase;
  });

  // [!code ++:1]
  signUpForm.append(nameInput, signInButton, signUpButton, passphraseDisplay);
  if (!authSecretStorage.isAuthenticated) {
    return signUpForm;
  }
  return null;
};
