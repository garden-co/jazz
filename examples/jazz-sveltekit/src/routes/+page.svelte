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
</script>

<div>
  <h1>SSR rendering example with Jazz</h1>
  <p>
    Data is still loaded only on the client, the components are rendered on the
    server with all the CoValues as null
  </p>
  <label>
    <p>Your profile name</p>
    <small>(only loaded on the client)</small>
    <input
      value={me.$isLoaded ? me.profile.name : ""}
      onchange={(e) => {
        if (!me.$isLoaded || !e.target) {
          return;
        }

        me.profile.$jazz.set("name", (e.target as HTMLInputElement).value);
      }}
    />
  </label>
  <button
    onclick={() => me.$isLoaded && goto(`/profile/${me.profile.$jazz.id}`)}
  >
    Your profile name in a Server Component &rarr;
  </button>
</div>
