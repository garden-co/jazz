<script lang="ts">
  import { getDb, getSession, QuerySubscription } from "jazz-tools/svelte";
  import { app } from "../schema.js";
  import { getStableHue } from "./constants.js";
  import { getAudioContext } from "./audio-context.js";

  let { jamId }: { jamId: string } = $props();

  const db = getDb();
  const session = getSession();
  const audio = getAudioContext();
  const allParticipants = new QuerySubscription(app.participants);
  const jamParticipants = $derived(
    (allParticipants.current ?? []).filter((p) => p.jam === jamId),
  );

  let editingId = $state<string | null>(null);
  let editValue = $state("");

  function startEditing(participant: { id: string; display_name: string }) {
    editingId = participant.id;
    editValue = participant.display_name;
  }

  function commitEdit() {
    if (editingId && editValue.trim()) {
      const name = editValue.trim();
      db.update(app.participants, editingId, { display_name: name });
      localStorage.setItem("wequencer-name", name);
    }
    editingId = null;
  }

  function onEditKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") {
      commitEdit();
    } else if (e.key === "Escape") {
      editingId = null;
    }
  }
</script>

<section class="participants">
  <h2>The Band</h2>

  {#each jamParticipants as participant (participant.id)}
    {@const isMe = participant.user_id === session?.user_id}
    <div class="participant" class:is-me={isMe}>
      <div
        class="avatar"
        style="background-color: oklch(0.6 0.15 {getStableHue(
          participant.user_id,
        )})"
      ></div>
      {#if isMe && editingId === participant.id}
        <!-- svelte-ignore a11y_autofocus this only displays when you click the button -->
        <input
          class="name-input"
          type="text"
          bind:value={editValue}
          onblur={commitEdit}
          onkeydown={onEditKeydown}
          autofocus
        />
      {:else}
        <button
          class="name"
          class:editable={isMe}
          onclick={() => isMe && startEditing(participant)}
          >{participant.display_name}
        </button>
      {/if}
      {#if isMe}
        <span class="you-tag">(you)</span>
      {/if}
    </div>
  {/each}

  <label class="sync-toggle">
    <input
      type="checkbox"
      checked={audio.syncAlignment}
      onchange={(e) => audio.setSyncAlignment(e.currentTarget.checked)}
    />
    Sync playback
  </label>
</section>
