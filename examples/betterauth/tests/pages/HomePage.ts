import { type Page, type Locator, expect } from "@playwright/test";

export class HomePage {
  page: Page;
  usernameInput: Locator;
  emailInput: Locator;
  passwordInput: Locator;
  confirmPasswordInput: Locator;
  signUpButton: Locator;
  signInButton: Locator;
  signUpLink: Locator;
  signInLink: Locator;
  logoutButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.usernameInput = this.page.getByRole("textbox", {
      name: "Name",
      exact: true,
    });
    this.emailInput = this.page.getByRole("textbox", {
      name: "Email",
      exact: true,
    });
    this.passwordInput = this.page.getByRole("textbox", {
      name: "Password",
      exact: true,
    });
    this.confirmPasswordInput = this.page.getByRole("textbox", {
      name: "Confirm password",
      exact: true,
    });
    this.signUpButton = this.page.getByRole("button", {
      name: "Sign up",
      exact: true,
    });
    this.signInButton = this.page.getByRole("button", {
      name: "Sign in",
      exact: true,
    });
    this.signUpLink = this.page.getByRole("link", {
      name: "Sign up",
      exact: true,
    });
    this.signInLink = this.page.getByRole("link", {
      name: "Sign in",
      exact: true,
    });
    this.logoutButton = this.page.getByRole("button", {
      name: "Sign out",
      exact: true,
    });
  }

  async signUpEmail(name: string, email: string, password: string) {
    await this.usernameInput.fill(name);
    await this.emailInput.fill(email);
    await this.passwordInput.fill(password);
    await this.confirmPasswordInput.fill(password);
    await this.signUpButton.click();
  }

  async signInEmail(email: string, password: string) {
    await this.emailInput.fill(email);
    await this.passwordInput.fill(password);
    await this.signInButton.click();
  }

  async logout() {
    await this.logoutButton.click();
  }

  async expectLoggedIn(name?: string) {
    await expect(this.logoutButton).toBeVisible();
    await expect(this.signInLink).not.toBeVisible();
    await expect(this.signUpLink).not.toBeVisible();
    if (name) {
      await expect(this.page.getByText(`Signed in as ${name}`)).toBeVisible();
    }
  }

  async expectLoggedOut() {
    await expect(this.logoutButton).not.toBeVisible();
    await expect(this.signInLink).toBeVisible();
    await expect(this.signUpLink).toBeVisible();
    await expect(this.page.getByText(`Anonymous user`)).toBeVisible();
  }
}
