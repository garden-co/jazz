import { ChatPage } from './pages/ChatPage';
import { test } from '@playwright/test';

test('chat works between two windows', async ({ page: marioPage, browser }) => {
  await marioPage.goto('/');
  const context = await browser.newContext();
  const luigiPage = await context.newPage();

  await marioPage.waitForURL('/chat/**');
  const roomUrl = marioPage.url();
  await luigiPage.goto(roomUrl);

  const marioChat = new ChatPage(marioPage);
  const luigiChat = new ChatPage(luigiPage);


  await marioChat.setUsername('Mario');

  const message1ByMario = 'Hello Luigi, are you ready to save the princess?';

  await marioChat.sendMessage(message1ByMario);
  await marioChat.expectMessageRow(message1ByMario);

  const roomURL = marioPage.url();
  await luigiPage.goto(roomURL);

  await luigiChat.setUsername('Luigi');

  await luigiChat.expectMessageRow(message1ByMario);

  const message2ByLuigi = "No, I'm not ready yet. I'm still trying to find the key to the castle.";

  await luigiChat.sendMessage(message2ByLuigi);
  await luigiChat.expectMessageRow(message2ByLuigi);

  await marioChat.expectMessageRow(message1ByMario);
  await luigiChat.expectMessageRow(message2ByLuigi);
  await context.close();
});
