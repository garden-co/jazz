import { type Page, expect } from "@playwright/test";

export class HomePage {
  constructor(public page: Page) {}

  usernameInput = this.page.getByRole("textbox", {
    name: "Name",
    exact: true,
  });
  emailInput = this.page.getByRole("textbox", {
    name: "Email",
    exact: true,
  });
  passwordInput = this.page.getByRole("textbox", {
    name: "Password",
    exact: true,
  });
  confirmPasswordInput = this.page.getByRole("textbox", {
    name: "Confirm password",
    exact: true,
  });
  signUpButton = this.page.getByRole("button", {
    name: "Sign up",
    exact: true,
  });
  signInButton = this.page.getByRole("button", {
    name: "Sign in",
    exact: true,
  });
  signUpLink = this.page.getByRole("link", {
    name: "Sign up",
    exact: true,
  });
  signInLink = this.page.getByRole("link", {
    name: "Sign in",
    exact: true,
  });
  logoutButton = this.page.getByRole("button", {
    name: "Sign out",
    exact: true,
  });

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
