import { Page, expect } from "@playwright/test";

export class HomePage {
  constructor(public page: Page) {}

  newPlaylistButton = this.page.getByRole("button", {
    name: "New Playlist",
  });

  playlistTitleInput = this.page.getByRole("textbox", {
    name: "Playlist title",
  });

  loginButton = this.page.getByRole("button", {
    name: "Sign up",
  });

  logoutButton = this.page.getByRole("button", {
    name: "Sign out",
  });

  async expectActiveTrackPlaying() {
    await expect(
      this.page.getByRole("button", {
        name: `Pause active track`,
      }),
    ).toBeVisible({
      timeout: 10_000,
    });
  }

  async expectMusicTrack(trackName: string) {
    await expect(
      this.page.getByRole("button", {
        name: `Play ${trackName}`,
      }),
    ).toBeVisible();
  }

  async notExpectMusicTrack(trackName: string) {
    await expect(
      this.page.getByRole("button", {
        name: `Play ${trackName}`,
      }),
    ).not.toBeVisible();
  }

  async playMusicTrack(trackName: string) {
    await this.page
      .getByRole("button", {
        name: `Play ${trackName}`,
      })
      .click();
  }

  async editTrackTitle(trackTitle: string, newTitle: string) {
    await this.page
      .getByRole("textbox", {
        name: `Edit track title: ${trackTitle}`,
      })
      .fill(newTitle);
  }

  async createPlaylist() {
    await this.newPlaylistButton.click();
  }

  async editPlaylistTitle(playlistTitle: string) {
    await this.playlistTitleInput.fill(playlistTitle);
  }

  async navigateToPlaylist(playlistTitle: string) {
    await this.page
      .getByRole("link", {
        name: playlistTitle,
      })
      .click();
  }

  async navigateToHome() {
    await this.page
      .getByRole("link", {
        name: "All tracks",
      })
      .click();
  }

  async getShareLink() {
    await this.page
      .getByRole("button", {
        name: "Share playlist",
      })
      .click();

    const inviteUrl = await this.page.evaluate(() =>
      navigator.clipboard.readText(),
    );

    expect(inviteUrl).toBeTruthy();

    return inviteUrl;
  }

  async addTrackToPlaylist(trackTitle: string, playlistTitle: string) {
    await this.page
      .getByRole("button", {
        name: `Open ${trackTitle} menu`,
      })
      .click();

    await this.page
      .getByRole("menuitem", {
        name: `Add to ${playlistTitle}`,
      })
      .click();
  }

  async removeTrackFromPlaylist(trackTitle: string, playlistTitle: string) {
    await this.page
      .getByRole("button", {
        name: `Open ${trackTitle} menu`,
      })
      .click();

    await this.page
      .getByRole("menuitem", {
        name: `Remove from ${playlistTitle}`,
      })
      .click();
  }

  async signUp(name: string) {
    await this.page.getByRole("button", { name: "Sign up" }).click();
    await this.page.getByRole("textbox", { name: "Username" }).fill(name);
    await this.page
      .getByRole("button", { name: "Sign up with passkey" })
      .click();

    await this.logoutButton.waitFor({
      state: "visible",
    });

    await expect(this.logoutButton).toBeVisible();
  }

  async logOut() {
    await this.logoutButton.click();

    await this.loginButton.waitFor({
      state: "visible",
    });

    await expect(this.loginButton).toBeVisible();
  }
}
