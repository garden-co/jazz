<script lang="ts">
  import { createImage } from 'jazz-tools/media';
  import { AccountCoState, CoState } from 'jazz-tools/svelte';
  import { Account, CoPlainText, getLoadedOrUndefined, type ID } from 'jazz-tools';

  import { page } from '$app/state';

  import { Chat, Message } from '$lib/schema';

  import AppContainer from '$lib/components/AppContainer.svelte';
  import ChatBody from '$lib/components/ChatBody.svelte';
  import ChatBubble from '$lib/components/ChatBubble.svelte';
  import EmptyChatMessage from '$lib/components/EmptyChatMessage.svelte';
  import ImageInput from '$lib/components/ImageInput.svelte';
  import InputBar from '$lib/components/InputBar.svelte';
  import TopBar from '$lib/components/TopBar.svelte';
  import TextInput from '$lib/components/TextInput.svelte';

  const chatId = $derived(page.params.id) as ID<typeof Chat>;
  const chat = $derived(
    new CoState(Chat, chatId, {
      resolve: {
        $each: {
          text: true
        }
      }
    })
  );
  const account = new AccountCoState(Account, {
    resolve: {
      profile: true
    }
  });
  const me = $derived(account.current);
  let showNLastMessages = $state(30);
  const sendImage = (event: Event & { currentTarget: HTMLInputElement }) => {
    const file = event.currentTarget.files?.[0];

    if (!file || !chat.current.$isLoaded) return;

    if (file.size > 5000000) {
      alert('Please upload an image less than 5MB.');
      return;
    }

    createImage(file, { owner: chat.current.$jazz.owner }).then((image) => {
      if (!chat.current.$isLoaded) return;
      chat.current.$jazz.push(
        Message.create(
          {
            text: CoPlainText.create(file.name, chat.current.$jazz.owner),
            image: image
          },
          chat.current.$jazz.owner
        )
      );
    });
  };
</script>

<AppContainer>
  <TopBar>
    <input
      type="text"
      value={getLoadedOrUndefined(me)?.profile?.name ?? ''}
      class="bg-transparent"
      onchange={(e) => {
        if (!me.$isLoaded) return;
        const target = e.target as HTMLInputElement;
        me.profile.$jazz.set('name', target.value);
      }}
      placeholder="Set username"
    />
    <button
      onclick={() => {
        account.logOut();
        window.location.reload(); // Otherwise the provider will not update with default profile name
      }}>Log out</button
    >
  </TopBar>
  {#if !chat}
    <div class="flex items-center justify-center flex-1">Loading...</div>
  {:else}
    <ChatBody>
      {#if chat.current.$isLoaded && chat.current.length > 0}
        {#each chat.current.slice(-showNLastMessages).reverse() as msg (msg.$jazz.id)}
          <ChatBubble {me} {msg} />
        {/each}
      {:else}
        <EmptyChatMessage />
      {/if}
      {#if chat.current.$isLoaded && chat.current.length > showNLastMessages}
        <button
          class="block px-4 py-1 mx-auto my-2 border rounded"
          onclick={() => (showNLastMessages += 10)}
        >
          Show more
        </button>
      {/if}
    </ChatBody>
    <InputBar>
      <ImageInput onImageChange={sendImage} />
      <TextInput
        onSubmit={(text: string) => {
          console.log({text})
          if (!chat.current.$isLoaded) return;
          chat.current.$jazz.push(
            Message.create(
              { text: CoPlainText.create(text, chat.current.$jazz.owner) },
              chat.current.$jazz.owner
            )
          );
        }}
      />
    </InputBar>
  {/if}
</AppContainer>
