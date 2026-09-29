<template>
  <Dialog :title="`Join ${bandName}`" @close="emit('done')">
    <form class="form" @submit.prevent="join">
      <p class="text-secondary">
        Members plan the tour together: they see tentative dates and private notes, and can add
        and move stops.
      </p>
      <label class="field">
        <span class="field__label">Your name</span>
        <input v-model="name" class="input" required autocomplete="name" />
        <span class="field__hint">The rest of the band sees this.</span>
      </label>
      <p v-if="error" class="field__error" role="alert">{{ error }}</p>
      <div class="actions">
        <Button variant="primary" type="submit" :disabled="joining">
          {{ joining ? "Joining…" : "Join band" }}
        </Button>
        <Button @click="emit('done')">Not now</Button>
      </div>
    </form>
  </Dialog>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { useDb } from "jazz-tools/vue";
import { app } from "../../schema.js";
import Button from "./ui/Button.vue";
import Dialog from "./ui/Dialog.vue";

const props = defineProps<{ bandId: string; bandName: string; code: string; userId: string }>();
const emit = defineEmits<{ done: [] }>();

const db = useDb();
const name = ref("");
const joining = ref(false);
const error = ref("");

// The members policy only accepts this row while the code matches the band's
// current invite, so the server is what decides whether the link still works.
async function join() {
  joining.value = true;
  error.value = "";
  try {
    await db
      .insert(app.members, {
        bandId: props.bandId,
        userId: props.userId,
        name: name.value.trim(),
        inviteCode: props.code,
      })
      .wait({ tier: "global" });
    emit("done");
  } catch {
    error.value = "This invite link no longer works. Ask the band owner for a new one.";
  } finally {
    joining.value = false;
  }
}
</script>
