import { Page, expect } from "@playwright/test";

export class HomePage {
  constructor(public page: Page) {}

  usernameInput = this.page.getByRole("textbox", {
    name: "Username (for registration)",
  });
  emailInput = this.page.getByRole("textbox", { name: "Email" });
  passwordInput = this.page.getByRole("textbox", { name: "Password" });
  signUpButton = this.page.getByRole("button", { name: "Sign up" });
  signInButton = this.page.getByRole("button", { name: "Sign in" });
  logoutButton = this.page.getByRole("button", {
    name: "Logout",
  });

  async signUp(name: string, email: string, password: string) {
    await this.usernameInput.fill(name);
    await this.emailInput.fill(email);
    await this.passwordInput.fill(password);
    await this.signUpButton.click();
  }

  async signIn(email: string, password: string) {
    await this.emailInput.fill(email);
    await this.passwordInput.fill(password);
    await this.signInButton.click();
  }

  async logout() {
    await this.logoutButton.click();
  }

  async expectLoggedIn(name: string | undefined = undefined) {
    await expect(this.logoutButton).toBeVisible();
    await expect(
      this.page.getByRole("heading", { name: "You're logged in" }),
    ).toBeVisible();
    if (name) {
      await expect(this.page.getByText(`Welcome back, ${name}`)).toBeVisible();
    }
  }

  async expectLoggedOut() {
    await expect(this.signInButton).toBeVisible();
  }
}
