<script lang="ts">
  import type { CoPlainText } from 'jazz-tools';

  let { text, className }: { text: CoPlainText | string | null; className?: string } = $props();
</script>

<div class="whitespace-pre-wrap"><p class="px-2 leading-relaxed {className}">{text}</p></div>
