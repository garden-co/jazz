<template>
  <div class="stack-6">
    <section class="stack-2">
      <h3 class="heading-4">Members</h3>
      <ul class="member-list">
        <li v-for="member in members ?? []" :key="member.id" class="row-between">
          <span>
            {{ member.name }}
            <span v-if="member.userId === userId" class="text-secondary">(you)</span>
          </span>
          <span v-if="member.userId === band.ownerId" class="badge">Owner</span>
          <Button v-else-if="isOwner" variant="ghost" @click="revoke(member.id)">Remove</Button>
        </li>
      </ul>
      <p v-if="isOwner" class="text-supporting">Removing a member also resets the invite link.</p>
    </section>

    <section v-if="isOwner" class="stack-2">
      <h3 class="heading-4">Invite link</h3>
      <p class="text-supporting">
        Anyone who opens this link can join the band and see tentative dates and private notes.
      </p>
      <div class="field-row field-row--tight">
        <input
          class="input"
          :value="inviteLink"
          readonly
          aria-label="Invite link"
          @focus="($event.target as HTMLInputElement).select()"
        />
        <Button :disabled="!inviteLink" @click="copy">{{ copied ? "Copied" : "Copy" }}</Button>
      </div>
      <div class="actions">
        <Button variant="ghost" @click="resetInvite">Reset link</Button>
      </div>
    </section>

    <section class="stack-2">
      <h3 class="heading-4">Public page</h3>
      <p class="text-supporting">
        Visitors who aren't in the band see confirmed dates only.
        <a :href="publicLink">Open the public page</a> in a private window to check.
      </p>
    </section>

    <div v-if="!isOwner && membership" class="actions">
      <Button variant="destructive" @click="leave">Leave band</Button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useAll, useDb } from "jazz-tools/vue";
import { app, type Band } from "../../schema.js";
import { bandLink, inviteLink as buildInviteLink } from "../lib/routes.js";
import { newInviteCode } from "../seed-loader.js";
import Button from "./ui/Button.vue";

const props = defineProps<{ band: Band; userId: string }>();

const db = useDb();
const isOwner = computed(() => props.band.ownerId === props.userId);
const { data: members } = useAll(() =>
  app.members.where({ bandId: props.band.id }).orderBy("name", "asc"),
);
// Only the owner can read invites; for everyone else this stays empty.
const { data: invites } = useAll(() => app.bandInvites.where({ bandId: props.band.id }).limit(1));

const membership = computed(() => members.value?.find((m) => m.userId === props.userId));
const invite = computed(() => invites.value?.[0]);
const inviteLink = computed(() =>
  invite.value?.code ? buildInviteLink(props.band.id, invite.value.code) : "",
);
const publicLink = computed(() => bandLink(props.band.id));

const copied = ref(false);
async function copy() {
  await navigator.clipboard.writeText(inviteLink.value);
  copied.value = true;
  setTimeout(() => (copied.value = false), 2000);
}

function resetInvite() {
  if (invite.value) db.delete(app.bandInvites, invite.value.id);
  db.insert(app.bandInvites, { bandId: props.band.id, code: newInviteCode() });
}

// A kept invite link would let a removed member straight back in, so revoking resets it.
function revoke(memberId: string) {
  db.delete(app.members, memberId);
  resetInvite();
}

function leave() {
  if (membership.value) db.delete(app.members, membership.value.id);
}
</script>
