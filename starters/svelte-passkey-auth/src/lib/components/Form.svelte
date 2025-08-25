<script lang="ts">
  import { JazzAccount } from "$lib/schema";
  import { AccountCoState } from "jazz-tools/svelte";

  const account = new AccountCoState(JazzAccount, {
    resolve: {
      profile: true,
      root: true,
    },
  });
  const me = $derived(account.current);

  const dateOfBirth = $derived(me ? me.root?.dateOfBirth?.toISOString().split("T")[0] || "" : "");

  function handleDateOfBirthChange(event: Event & { currentTarget: HTMLInputElement }) {
    if (me && event.currentTarget.value) {
      me.root.$jazz.set("dateOfBirth", new Date(event.currentTarget.value));
    }
  }
</script>

{#if me}
  <div class="grid gap-4 border p-8 border-stone-200">
    <div class="flex items-center gap-3">
      <label for="firstName" class="sm:w-32"> First name </label>
      <input
        type="text"
        id="firstName"
        placeholder="Enter your first name here..."
        class="border border-stone-300 rounded shadow-xs py-1 px-2 flex-1"
        bind:value={
          () => me.profile.firstName,
          newValue => me.profile.$jazz.set("firstName", newValue)
        }
      />
    </div>

    <div class="flex items-center gap-3">
      <label for="dateOfBirth" class="sm:w-32"> Date of birth </label>
      <input
        type="date"
        id="dateOfBirth"
        class="border border-stone-300 rounded shadow-xs py-1 px-2 flex-1"
        value={dateOfBirth}
        onchange={handleDateOfBirthChange}
      />
    </div>

    <!--Add more fields here-->
  </div>
{/if}
