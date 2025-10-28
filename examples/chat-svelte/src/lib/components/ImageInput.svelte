<script lang="ts">
  import type { ChangeEventHandler } from 'svelte/elements';

  import ImageIcon from '$lib/components/ImageIcon.svelte';
  let { onImageChange }: { onImageChange?: ChangeEventHandler<HTMLInputElement> } = $props();

  let input = $state<HTMLInputElement>();

  const onUploadClick = () => {
    input?.click();
  };
</script>

<button
  type="button"
  aria-label="Send image"
  title="Send image"
  onclick={onUploadClick}
  class="text-stone-500 p-1.5 rounded-full hover:bg-stone-100 hover:text-stone-800 dark:hover:bg-stone-800 dark:hover:text-stone-200 transition-colors"
>
  <ImageIcon size={24} strokeWidth={1.5} />
</button>

<label class="sr-only">
  Image
  <input
    bind:this={input}
    type="file"
    accept="image/png, image/jpeg, image/gif"
    onchange={onImageChange}
  />
</label>
