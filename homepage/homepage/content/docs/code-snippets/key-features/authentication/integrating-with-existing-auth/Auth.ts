import { createJazzBrowserContext } from "jazz-tools/browser";

let jazzContext;

const usernameInput = Object.assign(document.createElement("input"), {
  name: "username",
  type: "text",
  placeholder: "Username",
  required: true,
});

const passwordInput = Object.assign(document.createElement("input"), {
  name: "password",
  type: "password",
  placeholder: "Password",
  required: true,
});

const submitButton = Object.assign(document.createElement("button"), {
  type: "submit",
  textContent: "Login",
});

const form = Object.assign(document.createElement("form"), {
  onsubmit: async () => {
    // @ts-expect-error Virtual implementation
    const myOldAppUser = await myApp.logIn(
      usernameInput.value,
      passwordInput.value,
    );
    const accountID = myOldAppUser.jazzAccountID;
    // If you've stored this in an encrypted form, make sure to decrypt it first
    const accountSecret = myOldAppUser.jazzAccountSecret;
    // @ts-expect-error typings from oldAppUser.
    jazzContext = await createJazzBrowserContext({
      credentials: {
        accountID,
        accountSecret,
      },
    });
  },
});
