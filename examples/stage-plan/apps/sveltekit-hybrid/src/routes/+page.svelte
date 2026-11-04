<script lang="ts">
  import { getDb, getSession, QuerySubscription } from "jazz-tools/svelte";
  import { app } from "../../schema";
  import Account from "$lib/components/Account.svelte";
  import Board from "$lib/components/Board.svelte";
  import { createShow, type Me } from "$lib/model/actions";

  const db = getDb();
  const session = getSession();
  const account = $derived(session.current?.user.account ?? null);
  const profiles = new QuerySubscription(() =>
    account ? app.crew.where({ account }) : undefined,
  );
  const me = $derived<Me | undefined>(
    account && profiles.current?.[0] ? { account, profile: profiles.current[0] } : undefined,
  );
  const shows = new QuerySubscription(app.shows);
  let selectedId = $state<string>();
  const selected = $derived(
    shows.current?.find((show) => show.id === selectedId) ?? shows.current?.[0],
  );

  let profileName = $state("");
  let show = $state({ name: "", venue: "", date: "", doors: "19:00" });

  function saveProfile(event: SubmitEvent) {
    event.preventDefault();
    if (!account || !profileName.trim()) return;
    db.insert(app.crew, { account, name: profileName.trim() });
  }

  async function addShow(event: SubmitEvent) {
    event.preventDefault();
    if (!me || !show.name.trim()) return;
    const created = await createShow(db, me, { ...show, name: show.name.trim() });
    selectedId = created.show.id;
    show = { name: "", venue: "", date: "", doors: "19:00" };
  }
</script>

<main class="shell">
  <header class="top">
    <h1>StagePlan</h1>
    <Account />
  </header>

  {#if !account || !profiles.current}
    <p class="status" role="status">Loading your crew profile…</p>
  {:else if !me}
    <form class="card" onsubmit={saveProfile}>
      <h2>Set up your crew profile</h2>
      <label>Your name <input bind:value={profileName} required autocomplete="name" /></label>
      <button class="primary" type="submit">Continue</button>
    </form>
  {:else}
    <section class="card" aria-label="Shows">
      <h2>Shows</h2>
      {#if shows.current?.length}
        <ul class="shows">
          {#each shows.current as item (item.id)}
            <li>
              <button aria-current={item.id === selected?.id} onclick={() => (selectedId = item.id)}>
                {item.name}
              </button>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="secondary">No shows yet. Add your first one.</p>
      {/if}
      <form onsubmit={addShow}>
        <label>Show <input bind:value={show.name} required placeholder="Album launch" /></label>
        <label>Venue <input bind:value={show.venue} required placeholder="The Lantern" /></label>
        <label>Date <input type="date" bind:value={show.date} required /></label>
        <label>Doors <input type="time" bind:value={show.doors} required /></label>
        <button class="primary" type="submit">Add show</button>
      </form>
    </section>
    {#if selected}
      {#key selected.id}<Board show={selected} {me} />{/key}
    {/if}
  {/if}
</main>
