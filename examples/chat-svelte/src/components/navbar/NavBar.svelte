<script lang="ts">
  import { AccountCoState } from 'jazz-tools/svelte';
  import * as DropdownMenu from '@/components/ui/dropdown-menu';
  import { Menu, List, UserPen } from 'lucide-svelte';
  import Avatar from '@/components/Avatar.svelte';
  import Profile from '@/components/profile/Profile.svelte';
  import AuthModal from '@/components/navbar/AuthModal.svelte';
  import { Button } from '@/components/ui/button';
  import { ChatAccountWithProfile } from '@/lib/schema';
  import { navigate } from '@/lib/router';
  import { inIframe } from '@/lib/utils';

  const account = new AccountCoState(ChatAccountWithProfile);
  const me = $derived(account.current);
  const isAuthenticated = $derived(account.isAuthenticated);

  let menuOpen = $state(false);
  let profileOpen = $state(false);

  function closeMenu() {
    menuOpen = false;
  }

  function openProfile() {
    closeMenu();
    profileOpen = true;
  }
</script>

<header class="px-3 pt-2 pb-3 flex items-center gap-2">
  {#if me.$isLoaded}
    {#if inIframe}
      <div class="mx-auto flex items-center gap-2">
        <Avatar profileId={me.profile.$jazz.id} />
        <h3>{me.profile.name}</h3>
      </div>
    {:else}
      <DropdownMenu.DropdownMenu bind:open={menuOpen}>
        <DropdownMenu.DropdownMenuTrigger
          class="flex gap-2 items-center focus-visible:outline-0 cursor-pointer"
        >
          <Menu data-testid="menu" />
          <Avatar profileId={me.profile.$jazz.id} />
          <h3>{me.profile.name}</h3>
        </DropdownMenu.DropdownMenuTrigger>
        <DropdownMenu.DropdownMenuContent class="w-40" align="start">
          <DropdownMenu.DropdownMenuItem
            class="flex items-center gap-2"
            onSelect={(e) => {
              e.preventDefault();
              openProfile();
            }}
          >
            <UserPen class="size-4" />
            Profile
          </DropdownMenu.DropdownMenuItem>
          <DropdownMenu.DropdownMenuItem
            class="flex items-center gap-2"
            onSelect={() => {
              closeMenu();
              navigate('#/chats');
            }}
          >
            <List class="size-4" />
            Chat List
          </DropdownMenu.DropdownMenuItem>
        </DropdownMenu.DropdownMenuContent>
      </DropdownMenu.DropdownMenu>
      <div class="ms-auto">
        {#if isAuthenticated}
          <Button variant="ghost" onclick={() => account.logOut()}>Log out</Button>
        {:else}
          <AuthModal />
        {/if}
      </div>
    {/if}
  {:else}
    <div class="flex items-center gap-2 animate-pulse">
      <div class="w-10 h-10 bg-muted-foreground/20 rounded-full"></div>
      <div class="w-24 h-4 bg-muted-foreground/20 rounded"></div>
    </div>
  {/if}
</header>

<Profile open={profileOpen} onOpenChange={(v) => (profileOpen = v)} />
