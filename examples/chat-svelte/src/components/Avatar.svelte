<script lang="ts">
  import type { ID } from 'jazz-tools';
  import { createImage } from 'jazz-tools/media';
  import { AccountCoState, CoState, Image } from 'jazz-tools/svelte';
  import multiavatar from '@multiavatar/multiavatar/esm';
  import * as AvatarUi from '@/components/ui/avatar';
  import { Button } from '@/components/ui/button';
  import { ChatProfile } from '@/lib/schema';
  import { ChatAccountWithProfile } from '@/lib/schema';
  import clsx from 'clsx';

  interface Props {
    profileId?: ID<typeof ChatProfile>;
    editable?: boolean;
    class?: string;
  }

  let { profileId, editable = false, class: className = '' }: Props = $props();

  const account = new AccountCoState(ChatAccountWithProfile);
  const me = $derived(account.current);
  const actualProfileId = $derived(profileId ?? (me.$isLoaded ? me.profile.$jazz.id : undefined));

  const profileState = new CoState(ChatProfile, () => actualProfileId ?? null, {
    resolve: { avatar: true }
  });
  const profile = $derived(profileState.current);

  const avatarSvg = $derived(actualProfileId ? multiavatar(actualProfileId) : '');
  const dataUrl = $derived(avatarSvg ? `data:image/svg+xml;base64,${btoa(avatarSvg)}` : '');

  async function handleAvatarFile(e: Event) {
    const input = e.currentTarget as HTMLInputElement;
    const file = input.files?.[0];
    if (!file || !profile.$isLoaded) return;
    const img = await createImage(file, {
      progressive: true,
      maxSize: 256,
      placeholder: 'blur'
    });
    profile.$jazz.set('avatar', img);
    input.value = '';
  }

  function removeAvatar() {
    if (profile.$isLoaded) {
      profile.$jazz.delete('avatar');
    }
  }
</script>

{#if actualProfileId && profile.$isLoaded}
  {#if editable}
    <div class="space-y-2">
      <label for="avatar" class="text-sm font-medium">Avatar</label>
      <p class="text-muted-foreground text-sm">Upload a profile picture.</p>
      <div class="flex items-center gap-2">
        <label class="cursor-pointer transition-opacity hover:opacity-80">
          <AvatarUi.Avatar class={clsx('size-16 rounded-full overflow-hidden', className)}>
            {#if profile.avatar}
              <Image
                imageId={profile.avatar.$jazz.id}
                width={128}
                height="original"
                class="object-cover size-full"
              />
            {:else}
              <AvatarUi.AvatarImage src={dataUrl} alt="Avatar" />
            {/if}
            <AvatarUi.AvatarFallback>
              {profile.name
                ? profile.name
                    .split(' ')
                    .map((n) => n[0])
                    .join('')
                : '?'}
            </AvatarUi.AvatarFallback>
          </AvatarUi.Avatar>
          <input
            type="file"
            id="avatar"
            accept="image/*"
            class="hidden"
            onchange={handleAvatarFile}
          />
        </label>
        {#if profile.avatar}
          <Button variant="outline" type="button" onclick={removeAvatar}>Remove</Button>
        {/if}
      </div>
    </div>
  {:else}
    <AvatarUi.Avatar class={clsx('size-10 rounded-full overflow-hidden', className)}>
      {#if profile.avatar}
        <Image
          imageId={profile.avatar.$jazz.id}
          width={128}
          height="original"
          class="object-cover size-full"
        />
      {:else}
        <AvatarUi.AvatarImage src={dataUrl} alt="Avatar" />
      {/if}
      <AvatarUi.AvatarFallback>
        {profile.name
          ? profile.name
              .split(' ')
              .map((n) => n[0])
              .join('')
          : '?'}
      </AvatarUi.AvatarFallback>
    </AvatarUi.Avatar>
  {/if}
{:else}
  <AvatarUi.Avatar
    class={['size-10 rounded-full overflow-hidden', className].filter(Boolean).join(' ')}
  >
    <AvatarUi.AvatarFallback>?</AvatarUi.AvatarFallback>
  </AvatarUi.Avatar>
{/if}
