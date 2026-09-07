import { authClient } from "./auth-client.js";
import { getToken } from "./accounts.js";
import type { createJazzSession } from "jazz-tools/client";
type Session = Awaited<ReturnType<typeof createJazzSession>>;

export function mountSignUpForm(
  parent: HTMLElement,
  session: Session,
  onToggle: () => void,
  reportLinkFailure: (cause: unknown) => void,
): void {
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

    const { error } = await authClient.signUp.email({
      email,
      name,
      password,
    } as Parameters<typeof authClient.signUp.email>[0]);

    if (error) {
      errorEl.textContent = error.message ?? "Sign-up failed";
      errorEl.hidden = false;
      submit.disabled = false;
      submit.textContent = "Create account";
      return;
    }
    try {
      await session.linkJWT({ getToken });
    } catch (cause) {
      reportLinkFailure(cause);
      errorEl.textContent = cause instanceof Error ? cause.message : "Sign-up failed";
      errorEl.hidden = false;
    } finally {
      submit.disabled = false;
      submit.textContent = "Create account";
    }
  });
}
