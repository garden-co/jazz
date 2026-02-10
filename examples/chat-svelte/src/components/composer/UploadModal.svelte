<script lang="ts">
  import type { co, Group } from 'jazz-tools';
  import { CloudUpload } from 'lucide-svelte';
  import { Button } from '@/components/ui/button';
  import * as Dialog from '@/components/ui/dialog';
  import { Progress } from '@/components/ui/progress';
  import { cn } from '@/lib/utils';
  import { uploadFile } from '@/lib/utils';
  import { toast } from 'svelte-sonner';
  import { buttonVariants } from '@/components/ui/button';
  import { Attachment } from '@/lib/schema';

  interface Props {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    accept?: string;
    title: string;
    onUpload: (attachment: co.loaded<typeof Attachment>) => void;
    owner?: Group;
  }

  let { open, onOpenChange, accept, title, onUpload, owner }: Props = $props();

  let isUploading = $state(false);
  let progress = $state(0);

  async function handleFile(file: File) {
    try {
      isUploading = true;
      const uploaded = await uploadFile(file, {
        onProgress: (p) => (progress = p),
        owner
      });
      onUpload(uploaded);
      toast.success('Upload successful');
      onOpenChange(false);
    } catch (err) {
      console.error(err);
      toast.error('Upload failed');
    } finally {
      isUploading = false;
      progress = 0;
    }
  }
</script>

<Dialog.Root {open} onOpenChange={isUploading ? undefined : onOpenChange}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>{title}</Dialog.Title>
    </Dialog.Header>

    <label
      class={cn(
        buttonVariants({ variant: 'ghost' }),
        'border-border flex aspect-video w-full cursor-pointer flex-col items-center justify-center rounded-xl border-2 border-dashed transition-colors',
        isUploading ? 'cursor-not-allowed opacity-60' : 'hover:text-muted-foreground'
      )}
    >
      <CloudUpload class="size-1/3" />

      {#if isUploading}
        <div class="mt-2 w-full px-8 text-center">
          <p>Uploading…</p>
          <Progress value={progress} class="mt-2" />
        </div>
      {/if}

      <input
        type="file"
        hidden
        disabled={isUploading}
        {accept}
        onchange={(evt) => {
          const file = evt.currentTarget.files?.[0];
          if (!file) return;
          evt.currentTarget.value = '';
          handleFile(file);
        }}
      />
    </label>

    <Dialog.Footer>
      <Button variant="ghost" disabled={isUploading} onclick={() => onOpenChange(false)}>
        Cancel
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
