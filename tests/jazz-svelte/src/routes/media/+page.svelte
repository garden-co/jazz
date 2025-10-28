<script lang="ts">
  import type { ChangeEventHandler } from 'svelte/elements';
  import { AccountCoState, Image } from 'jazz-tools/svelte';
  import { createImage } from 'jazz-tools/media';
  import { TestAccount } from './schema.js';

  const me = new AccountCoState(TestAccount, {
    resolve: {
      profile: {
        image: true
      }
    }
  });

  let input = $state<HTMLInputElement>();

  const onUploadClick = () => {
    input?.click();
  };

  const onImageChange: ChangeEventHandler<HTMLInputElement> = (event) => {
    const file = event.currentTarget.files?.[0];
		if (!file || !me.current?.profile) return;
		createImage(file, {
			owner: me.current?.profile._owner,
			maxSize: 400
		}).then((image) => {
			if (!me.current?.profile) return;
			me.current.profile.image = image;
		});
  }
</script>

<button onclick={onUploadClick}>
  {me.current?.profile?.image ? 'Change image' : 'Send image'}
</button>

{#if me.current?.profile?.image}
  <Image imageId={me.current.profile.image.id} width={200} height="original" />
{/if}

<label>
  <input
    bind:this={input}
    type="file"
    accept="image/png, image/jpeg, image/gif"
    onchange={onImageChange}
  />
</label>

<style>
  label {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border-width: 0;
  }
</style>
