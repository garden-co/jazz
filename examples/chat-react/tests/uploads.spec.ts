import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "@playwright/test";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

test.describe("Upload Functionality", () => {
  test("Should upload an image and display it in chat", async ({ page }) => {
    await test.step("Initial Load", async () => {
      await page.goto("/");
      await page.waitForURL("**/#/chat/*");
    });

    await test.step("Open Image Upload Menu", async () => {
      await page.locator("button:has(.lucide-plus)").click();
      await page.getByRole("menuitem", { name: /image/i }).click();
    });

    const testImagePath = path.join(__dirname, "test-image.png");
    await test.step("Prepare and upload image", async () => {
      const tinyPng = Buffer.from(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==",
        "base64",
      );
      fs.writeFileSync(testImagePath, tinyPng);
      await page.locator('input[type="file"]').setInputFiles(testImagePath);
    });

    await test.step("Verify upload and cleanup", async () => {
      await expect(page.getByText(/upload successful/i)).toBeVisible();
      await expect(page.locator("article img").last()).toBeVisible();
      fs.unlinkSync(testImagePath);
    });
  });

  test("Should upload a file and display it in chat", async ({ page }) => {
    await test.step("Initial Load", async () => {
      await page.goto("/");
      await page.waitForURL("**/#/chat/*");
    });

    await test.step("Open File Upload Menu", async () => {
      await page.locator("button:has(.lucide-plus)").click();
      await page.getByRole("menuitem", { name: /file/i }).click();
    });

    const testFileName = "test-upload-file.txt";
    const testFilePath = path.join(__dirname, testFileName);
    await test.step("Prepare and upload file", async () => {
      fs.writeFileSync(testFilePath, "Hello upload test");
      await page.locator('input[type="file"]').setInputFiles(testFilePath);
    });

    await test.step("Verify file presence and cleanup", async () => {
      await expect(page.getByText(/upload successful/i)).toBeVisible();
      await expect(page.getByText(testFileName)).toBeVisible();
      await expect(
        page.getByRole("button", { name: /download/i }).last(),
      ).toBeVisible();
      fs.unlinkSync(testFilePath);
    });
  });
});
