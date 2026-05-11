import type { Db } from "jazz-tools";
import { authClient } from "./auth-client.js";

export function mountSignUpForm(parent: HTMLElement, db: Db, onToggle: () => void): void {
  parent.innerHTML = `
    <div class="card">
      <h1>Create account</h1>
      <form>
        <div class="field">
          <label for="name">Name</label>
          <input id="name" name="name" type="text" required />
        </div>
        <div class="field">
          <label for="email">Email</label>
          <input id="email" name="email" type="email" required />
        </div>
        <div class="field">
          <label for="password">Password</label>
          <input id="password" name="password" type="password" required />
        </div>
        <p class="alert-error" role="alert" data-slot="error" hidden></p>
        <button type="submit" class="btn-primary">Create account</button>
      </form>
      <p class="toggle">
        Already have an account?
        <button type="button" class="link" data-action="toggle">Sign in</button>
      </p>
    </div>
  `;

  const form = parent.querySelector<HTMLFormElement>("form")!;
  const errorEl = parent.querySelector<HTMLParagraphElement>('[data-slot="error"]')!;
  const submit = form.querySelector<HTMLButtonElement>("button[type='submit']")!;

  parent
    .querySelector<HTMLButtonElement>('[data-action="toggle"]')!
    .addEventListener("click", () => {
      onToggle();
    });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    errorEl.hidden = true;
    submit.disabled = true;
    submit.textContent = "Creating account…";

    const name = (form.elements.namedItem("name") as HTMLInputElement).value;
    const email = (form.elements.namedItem("email") as HTMLInputElement).value;
    const password = (form.elements.namedItem("password") as HTMLInputElement).value;

    const proofToken = await db.getLocalFirstIdentityProof({
      ttlSeconds: 60,
      audience: "react-localfirst-signup",
    });

    if (!proofToken) {
      errorEl.textContent = "Sign up requires an active Jazz session";
      errorEl.hidden = false;
      submit.disabled = false;
      submit.textContent = "Create account";
      return;
    }

    const { error } = await authClient.signUp.email({
      email,
      name,
      password,
      proofToken,
    } as Parameters<typeof authClient.signUp.email>[0]);

    submit.disabled = false;
    submit.textContent = "Create account";

    if (error) {
      errorEl.textContent = error.message ?? "Sign-up failed";
      errorEl.hidden = false;
    }
  });
}
