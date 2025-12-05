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
  const authSecretStorage = contextManager.getAuthSecretStorage();
  const appName = "JazzFest";

  const auth = new BrowserPasskeyAuth(
    crypto,
    authenticate,
    authSecretStorage,
    appName,
  );

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

  signUpForm.append(nameInput, signInButton, signUpButton);
  if (!authSecretStorage.isAuthenticated) {
    return signUpForm;
  }
  return null;
};
