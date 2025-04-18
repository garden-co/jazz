import { page, userEvent } from "@vitest/browser/context";
import { createImage } from "jazz-browser-media-images";
import { AuthSecretStorage } from "jazz-tools";
import { assert, afterEach, describe, expect, test } from "vitest";
import { createAccountContext, startSyncServer } from "./testUtils";

describe("Images upload", () => {
  afterEach(async () => {
    await new AuthSecretStorage().clear();
  });

  test("should upload images", async () => {
    const syncServer = await startSyncServer();

    await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
    });

    document.body.innerHTML = `
      <input type="file" id="fileInput" />
    `;

    const input = document.getElementById("fileInput") as HTMLInputElement;

    let file: File | null = null;

    input.addEventListener("change", (event) => {
      file = (event.target as HTMLInputElement).files?.[0] || null;
    });

    await userEvent.upload(
      page.getByRole("textbox"),
      "./fixtures/jazz-icon.png",
    );

    assert(file);

    const image = await createImage(file);

    const highestRes = image.highestResAvailable();

    assert(highestRes);

    expect(highestRes.res).toBe("512x512");

    const blob = highestRes.stream.toBlob();

    assert(blob);

    const blobURI = URL.createObjectURL(blob);

    const img = document.createElement("img");
    img.src = blobURI;

    await new Promise<void>((resolve, reject) => {
      img.onload = () => resolve();
      img.onerror = () => reject();

      document.body.appendChild(img);
    });

    expect(img.src).toBe(blobURI);

    expect(img.naturalWidth).toBe(512);
    expect(img.naturalHeight).toBe(512);

    URL.revokeObjectURL(blobURI);
  });
});
