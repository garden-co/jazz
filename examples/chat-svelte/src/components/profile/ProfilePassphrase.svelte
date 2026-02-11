<script lang="ts">
  import { usePassphraseAuth } from 'jazz-tools/svelte';
  import { Eye, EyeOff } from 'lucide-svelte';
  import { Button } from '@/components/ui/button';
  import { Label } from '@/components/ui/label';
  import { wordlist } from '@/lib/wordlist';
  import { toast } from 'svelte-sonner';

  const passphraseAuth = usePassphraseAuth({ wordlist });
  let revealPassphrase = $state(false);

  const displayValue = $derived(
    revealPassphrase ? passphraseAuth.passphrase : passphraseAuth.passphrase.replace(/\S/g, '*')
  );
</script>

<div class="space-y-2">
  <Label for="passphrase">Passphrase</Label>
  <p class="text-muted-foreground text-sm">A secret code you can use to recover your account.</p>
  <Button
    variant="outline"
    type="button"
    name="reveal"
    onclick={() => (revealPassphrase = !revealPassphrase)}
  >
    {#if revealPassphrase}
      <EyeOff class="size-4" />
      Hide
    {:else}
      <Eye class="size-4" />
      Reveal
    {/if}
  </Button>
  <textarea
    id="passphrase"
    rows={4}
    class="text-muted-foreground mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm caret-transparent focus:ring-0 {revealPassphrase
      ? 'cursor-copy'
      : ''}"
    value={displayValue}
    readonly
    disabled={!revealPassphrase}
    onclick={() => {
      if (revealPassphrase) {
        navigator.clipboard.writeText(passphraseAuth.passphrase);
        toast.success('Passphrase copied to clipboard!');
      }
    }}
  ></textarea>
</div>
