import { expect, test } from "@playwright/test";

test("create and edit an order", async ({ page }) => {
  await page.goto("/");
  // start an order
  await page.getByRole("button", { name: "Create your first order" }).click();
  await page.getByLabel("Base tea").selectOption("Oolong");
  await page.getByLabel("Pearl").check();
  await page.getByLabel("Taro").check();
  await page.getByLabel("Delivery date").fill("2024-12-21");
  await page.getByLabel("With milk?").check();
  await page.getByLabel("Special instructions").fill("25% sugar");
  await page.getByRole("button", { name: "Submit" }).click();

  await page.waitForURL("/");

  // check if order was created correctly
  const firstOrder = page.getByRole("link", { name: "Oolong milk tea" });
  await expect(firstOrder).toHaveText(/25% sugar/);
  await expect(firstOrder).toHaveText(/12\/21\/2024/);
  await expect(firstOrder).toHaveText(/with pearl, taro/);

  // edit order
  await firstOrder.click();
  await page.getByLabel("Base tea").selectOption("Jasmine");
  await page.getByLabel("Red bean").check();
  await page.getByLabel("Brown sugar").check();
  await page.getByLabel("Delivery date").fill("2024-12-25");
  await page.getByLabel("With milk?").uncheck();
  await page.getByLabel("Special instructions").fill("10% sugar");
  await page.getByRole("link", { name: /Back to all orders/ }).click();

  // no autosave - the edit hasn't been submitted
  const sameOrder = page.getByRole("link", { name: "Oolong milk tea" });
  await expect(sameOrder).toHaveText(/25% sugar/);
  await expect(sameOrder).toHaveText(/12\/21\/2024/);
  await expect(sameOrder).toHaveText(/with pearl, taro/);

  await sameOrder.click();
  await page.getByLabel("Base tea").selectOption("Jasmine");
  await page.getByLabel("Red bean").check();
  await page.getByLabel("Brown sugar").check();
  await page.getByLabel("Delivery date").fill("2024-12-25");
  await page.getByLabel("With milk?").uncheck();
  await page.getByLabel("Special instructions").fill("10% sugar");
  await page.getByRole("button", { name: "Submit" }).click();

  await page.waitForURL("/");

  // check if order was submitted correctly
  const editedOrder = page.getByRole("link", { name: "Jasmine tea" });
  await expect(editedOrder).toHaveText(/10% sugar/);
  await expect(editedOrder).toHaveText(/12\/25\/2024/);
  await expect(editedOrder).toHaveText(
    /with pearl, taro, red bean, brown sugar/,
  );
});
