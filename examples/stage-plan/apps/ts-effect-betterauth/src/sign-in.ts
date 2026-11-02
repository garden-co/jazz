import { authClient } from "./auth-client";
import { field, h } from "./dom";

/** Sign in, or create an account; a session reloads into the chat. */
export function signInScreen(): HTMLElement {
  let signingUp = false;
  const root = h("main", { class: "shell" });

  function render(error?: string) {
    const name = h("input", { name: "name", required: true, autocomplete: "name" });
    const email = h("input", {
      name: "email",
      type: "email",
      required: true,
      autocomplete: "email",
    });
    const password = h("input", {
      name: "password",
      type: "password",
      required: true,
      minLength: 8,
      autocomplete: signingUp ? "new-password" : "current-password",
    });
    const submit = h(
      "button",
      { type: "submit", class: "primary" },
      signingUp ? "Create account" : "Sign in",
    );
    const form = h(
      "form",
      {
        class: "card sign-in",
        on: {
          submit: (event) => {
            event.preventDefault();
            submit.disabled = true;
            void (async () => {
              const { error } = await (signingUp
                ? authClient.signUp.email({
                    name: name.value,
                    email: email.value,
                    password: password.value,
                  })
                : authClient.signIn.email({ email: email.value, password: password.value }));
              if (error) render(error.message ?? "Sign-in failed");
              // The session atom updates and main.ts opens the planner.
            })();
          },
        },
      },
      h("h1", {}, signingUp ? "Create your StagePlan account" : "Sign in to StagePlan"),
      h(
        "p",
        { class: "secondary" },
        "Plan your shows with your crew: tasks for every gig, live on every device.",
      ),
      signingUp && field("Name", name),
      field("Email", email),
      field("Password", password),
      error && h("p", { class: "error", role: "alert" }, error),
      submit,
      h(
        "button",
        {
          type: "button",
          class: "ghost",
          on: {
            click: () => {
              signingUp = !signingUp;
              render();
            },
          },
        },
        signingUp ? "I already have an account" : "Create an account",
      ),
    );
    root.replaceChildren(form);
  }
  render();
  return root;
}
