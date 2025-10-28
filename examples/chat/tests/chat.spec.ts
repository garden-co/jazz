import { test } from "@playwright/test";
import { ChatPage } from "./pages/ChatPage";

test("chat between two users", async ({ page: marioPage, browser }) => {
  const context = await browser.newContext();
  const luigiPage = await context.newPage();

  await marioPage.goto("/");

  const marioChat = new ChatPage(marioPage);
  const luigiChat = new ChatPage(luigiPage);

  await marioChat.setUsername("Mario");

  const message1ByMario = "Hello Luigi, are you ready to save the princess?";

  await marioChat.sendMessage(message1ByMario);
  await marioChat.expectMessageRow(message1ByMario);

  const roomURL = marioPage.url();
  await luigiPage.goto(roomURL);

  await luigiChat.setUsername("Luigi");

  await luigiChat.expectMessageRow(message1ByMario);

  const message2ByLuigi =
    "No, I'm not ready yet. I'm still trying to find the key to the castle.";

  await luigiChat.sendMessage(message2ByLuigi);
  await luigiChat.expectMessageRow(message2ByLuigi);

  await marioChat.expectMessageRow(message1ByMario);
  await luigiChat.expectMessageRow(message2ByLuigi);
});
