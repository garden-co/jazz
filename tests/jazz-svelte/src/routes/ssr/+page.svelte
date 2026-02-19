<script lang="ts">
  import { goto } from "$app/navigation";
  import { Account } from "jazz-tools";
  import { AccountCoState } from "jazz-tools/svelte";

  const account = new AccountCoState(Account, {
    resolve: {
      profile: true,
    },
  });
  const me = $derived(account.current);

  let synced = $state(false);

  const waitForSync = async () => {
    if (me.$isLoaded) {
      await me.profile.$jazz.waitForSync();
      synced = true;
    }
  };

  const navigate = () => {
    if (me.$isLoaded) {
      goto(`/ssr/profile/${me.profile.$jazz.id}`);
    }
  };
</script>

{#if me.$isLoaded}
  <input
    data-testid="name-input"
    value={me.profile.name ?? ""}
    oninput={(e) => me.profile.$jazz.set("name", e.currentTarget.value)}
  />
  <button data-testid="navigate" onclick={navigate}> View Profile SSR </button>
  <p data-testid="profile-id">{me.profile.$jazz.id}</p>
{/if}
