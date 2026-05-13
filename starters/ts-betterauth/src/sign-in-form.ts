import { authClient } from "./auth-client.js";

type Mode = "signin" | "signup";

export function mountSignInForm(parent: HTMLElement): void {
  let mode: Mode = "signin";

  function render() {
    parent.innerHTML = `
      <div class="card">
        <h1>${mode === "signup" ? "Create account" : "Sign in"}</h1>
        <form>
          ${
            mode === "signup"
              ? `<div class="field">
                   <label for="name">Name</label>
                   <input id="name" name="name" type="text" required />
                 </div>`
              : ""
          }
          <div class="field">
            <label for="email">Email</label>
            <input id="email" name="email" type="email" required />
          </div>
          <div class="field">
            <label for="password">Password</label>
            <input id="password" name="password" type="password" required />
          </div>
          <p class="alert-error" role="alert" data-slot="error" hidden></p>
          <button type="submit" class="btn-primary">
            ${mode === "signup" ? "Create account" : "Sign in"}
          </button>
        </form>
        <p class="toggle">
          ${mode === "signup" ? "Already have an account?" : "New here?"}
          <button type="button" class="link" data-action="toggle">
            ${mode === "signup" ? "Sign in" : "Create an account"}
          </button>
        </p>
      </div>
    `;

    const form = parent.querySelector<HTMLFormElement>("form")!;
    const errorEl = parent.querySelector<HTMLParagraphElement>('[data-slot="error"]')!;
    const submit = form.querySelector<HTMLButtonElement>("button[type='submit']")!;

    parent
      .querySelector<HTMLButtonElement>('[data-action="toggle"]')!
      .addEventListener("click", () => {
        mode = mode === "signup" ? "signin" : "signup";
        render();
      });

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      errorEl.hidden = true;
      submit.disabled = true;

      const email = (form.elements.namedItem("email") as HTMLInputElement).value;
      const password = (form.elements.namedItem("password") as HTMLInputElement).value;

      const result =
        mode === "signup"
          ? await authClient.signUp.email({
              name: (form.elements.namedItem("name") as HTMLInputElement).value,
              email,
              password,
            })
          : await authClient.signIn.email({ email, password });

      submit.disabled = false;

      if (result.error) {
        errorEl.textContent =
          result.error.message ?? (mode === "signup" ? "Sign-up failed" : "Sign-in failed");
        errorEl.hidden = false;
      }
    });
  }

  render();
}
