<script lang="ts">
  import { usePasskeyAuth, usePassphraseAuth } from 'jazz-tools/svelte';
  import { Fingerprint } from 'lucide-svelte';
  import * as Dialog from '@/components/ui/dialog';
  import * as Tabs from '@/components/ui/tabs';
  import { Button } from '@/components/ui/button';
  import { wordlist } from '@/lib/wordlist';

  let open = $state(false);
  const passphraseAuth = usePassphraseAuth({ wordlist });
  const passkeyAuth = usePasskeyAuth({ appName: 'Jazz Chat' });
  let passphrase = $state('');
</script>

<Dialog.Root bind:open>
  <Dialog.DialogTrigger>
    {#snippet child({ props })}
      <Button variant="ghost" {...props}>Log In</Button>
    {/snippet}
  </Dialog.DialogTrigger>
  <Dialog.DialogPortal>
    <Dialog.DialogOverlay />
    <Dialog.DialogContent class="max-w-md">
      <Dialog.DialogTitle>Log In</Dialog.DialogTitle>
      <Tabs.Tabs value="passkey">
        <Tabs.TabsList class="mb-4">
          <Tabs.TabsTrigger value="passkey">Passkey</Tabs.TabsTrigger>
          <Tabs.TabsTrigger value="passphrase">Passphrase</Tabs.TabsTrigger>
        </Tabs.TabsList>
        <Tabs.TabsContent value="passkey">
          <div>
            <label for="passkey-login" class="text-sm font-medium">Passkey</label>
            <p class="text-muted-foreground text-sm">
              Passkeys are a secure way to log in without a password. If you have previously
              registered a passkey, you can use it to log in.
            </p>
            <Button
              id="passkey-login"
              variant="secondary"
              class="mt-2"
              onclick={() => passkeyAuth.current.logIn()}
            >
              <Fingerprint class="size-4" /> Log in using a passkey
            </Button>
          </div>
        </Tabs.TabsContent>
        <Tabs.TabsContent value="passphrase">
          <div class="space-y-4">
            <div>
              <label for="passphrase" class="text-sm font-medium">Passphrase</label>
              <p class="text-muted-foreground text-sm">
                If you have a passphrase, you can enter it below.
              </p>
              <textarea
                id="passphrase"
                rows={3}
                class="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                bind:value={passphrase}
              ></textarea>
            </div>
            <Button onclick={() => passphraseAuth.logIn(passphrase)}>
              Log in using passphrase
            </Button>
          </div>
        </Tabs.TabsContent>
      </Tabs.Tabs>
    </Dialog.DialogContent>
  </Dialog.DialogPortal>
</Dialog.Root>
