<script lang="ts">
  let { onSubmit }: { onSubmit: (text: string) => void } = $props();
  const inputId = $props.id();
</script>

<div class="flex-1">
  <label class="sr-only" for={inputId}> Type a message and press Enter </label>
  <input
    id={inputId}
    class="block w-full px-3 py-1 border rounded-full border-stone-200 placeholder:text-stone-500 dark:bg-stone-925 dark:text-white dark:border-stone-900"
    placeholder="Type a message and press Enter"
    maxLength={2048}
    type="text"
    onkeydown={({ key, currentTarget: input }) => {
      if (key !== 'Enter' || !input.value) return;
      onSubmit(input.value);
      input.value = '';
    }}
  />
</div>
