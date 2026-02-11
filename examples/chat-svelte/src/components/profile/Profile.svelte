<script lang="ts">
  import { AccountCoState } from 'jazz-tools/svelte';
  import { usePasskeyAuth } from 'jazz-tools/svelte';
  import { Fingerprint, LogOut } from 'lucide-svelte';
  import * as Sheet from '@/components/ui/sheet';
  import { Button } from '@/components/ui/button';
  import { Input } from '@/components/ui/input';
  import { Label } from '@/components/ui/label';
  import Avatar from '@/components/Avatar.svelte';
  import ProfilePassphrase from '@/components/profile/ProfilePassphrase.svelte';
  import { ChatAccountWithProfile } from '@/lib/schema';

  interface Props {
    open: boolean;
    onOpenChange: (open: boolean) => void;
  }

  let { open, onOpenChange }: Props = $props();

  const account = new AccountCoState(ChatAccountWithProfile);
  const me = $derived(account.current);
  const passkeyAuth = usePasskeyAuth({ appName: 'Jazz Chat' });
</script>

<Sheet.Root {open} {onOpenChange}>
  <Sheet.Content side="right" role="dialog">
    <Sheet.Header>
      <Sheet.Title>Your Account</Sheet.Title>
      <Sheet.Description class="sr-only">
        Manage your account settings and preferences.
      </Sheet.Description>
    </Sheet.Header>
    {#if me.$isLoaded}
      <div class="grid flex-1 auto-rows-min gap-6 px-4 overflow-y-auto">
        <div>
          <Label {...{ for: 'name' }}>Name</Label>
          <p class="text-muted-foreground text-sm">The name you would like to be known by.</p>
          <Input
            id="name"
            type="text"
            class="mt-1"
            value={me.profile.name}
            oninput={(e) => me.profile.$jazz.set('name', e.currentTarget.value)}
          />
        </div>
        <Avatar editable />
        <div>
          <Label {...{ for: 'passkey-register' }}>Passkey</Label>
          <p class="text-muted-foreground text-sm">
            A passkey allows you to log back in quickly and easily.
          </p>
          <Button
            id="passkey-register"
            variant="secondary"
            class="mt-2"
            onclick={async () => {
              await passkeyAuth.current.signUp(me.profile.name);
              onOpenChange(false);
            }}
          >
            <Fingerprint class="size-4" />
            Register a passkey
          </Button>
        </div>
        <ProfilePassphrase />
      </div>
      <Sheet.Footer>
        <Label {...{ for: 'logout' }}>Log Out</Label>
        <p class="text-muted-foreground text-sm">
          If you log out, you will be automatically provisioned with a new anonymous account.
        </p>
        <Button
          id="profile-logout"
          variant="destructive"
          class="mt-2"
          onclick={() => {
            account.logOut();
            onOpenChange(false);
          }}
        >
          <LogOut />
          Log out
        </Button>
      </Sheet.Footer>
    {:else}
      <p class="text-muted-foreground p-8 text-center italic">Loading account...</p>
    {/if}
  </Sheet.Content>
</Sheet.Root>
