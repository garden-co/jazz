import { Page, expect } from "@playwright/test";

export class HomePage {
  constructor(public page: Page) {}

  usernameInput = this.page.getByRole("textbox", {
    name: "Profile name",
  });
  emailInput = this.page.getByRole("textbox", { name: "Email address" });
  passwordInput = this.page.getByRole("textbox", {
    name: "Password",
    exact: true,
  });
  confirmPasswordInput = this.page.getByRole("textbox", {
    name: "Confirm password",
  });
  signUpButton = this.page.getByRole("button", { name: "Sign up" });
  signInButton = this.page.getByRole("button", { name: "Sign in" });
  logoutButton = this.page.getByRole("button", {
    name: "Logout",
  });

  async signUp(name: string, email: string, password: string) {
    await this.usernameInput.fill(name);
    await this.emailInput.fill(email);
    await this.passwordInput.fill(password);
    await this.confirmPasswordInput.fill(password);
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
    if (name) {
      await expect(this.page.getByText(`Welcome back, ${name}`)).toBeVisible();
    }
  }

  async expectLoggedOut() {
    await expect(this.signInButton).toBeVisible();
  }
}
