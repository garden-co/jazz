<script lang="ts">
  import type { ID } from 'jazz-tools';
  import { co } from 'jazz-tools';
  import { CoState } from 'jazz-tools/svelte';
  import { Download } from 'lucide-svelte';
  import { Button } from '@/components/ui/button';
  import { downloadBlob, formatBytes } from '@/lib/utils';
  import { toast } from 'svelte-sonner';

  interface Props {
    fileId: ID<typeof co.fileStream>;
  }

  let { fileId }: Props = $props();

  const fileStreamSchema = co.fileStream();
  const fileState = new CoState(fileStreamSchema, () => fileId);
  const file = $derived(fileState.current);

  const fileName = $derived(file.$isLoaded ? file.getMetadata()?.fileName : '');
  const fileSize = $derived(file.$isLoaded ? file.getMetadata()?.totalSizeBytes : 0);

  async function handleDownload() {
    if (!file.$isLoaded) return;
    const blob = file.toBlob();
    if (!blob) {
      toast.error('File was corrupted');
      return;
    }
    downloadBlob(blob, fileName || 'download');
  }
</script>

{#if file.$isLoaded}
  <div class="my-2 flex flex-col rounded-xl">
    <span class="mb-2 break-words">{fileName}</span>
    <Button variant="secondary" onclick={handleDownload}>
      <Download class="size-4" />
      Download
      {#if fileSize}
        <span class="text-sm">({formatBytes(fileSize)})</span>
      {/if}
    </Button>
  </div>
{:else}
  <div class="text-muted-foreground text-sm">{file.$jazz.loadingState ?? 'Loading'}...</div>
{/if}
