import { authClient } from "./auth-client.js";

export function mountSignInForm(parent: HTMLElement, onToggle: () => void): void {
  parent.innerHTML = `
    <div class="card">
      <h1>Sign in</h1>
      <form>
        <div class="field">
          <label for="email">Email</label>
          <input id="email" name="email" type="email" required />
        </div>
        <div class="field">
          <label for="password">Password</label>
          <input id="password" name="password" type="password" required />
        </div>
        <p class="alert-error" role="alert" data-slot="error" hidden></p>
        <button type="submit" class="btn-primary">Sign in</button>
      </form>
      <p class="toggle">
        New here?
        <button type="button" class="link" data-action="toggle">Create an account</button>
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

    const email = (form.elements.namedItem("email") as HTMLInputElement).value;
    const password = (form.elements.namedItem("password") as HTMLInputElement).value;

    const result = await authClient.signIn.email({ email, password });

    submit.disabled = false;

    if (result.error) {
      errorEl.textContent = result.error.message ?? "Sign-in failed";
      errorEl.hidden = false;
    }
  });
}
