<script lang="ts">
import { co } from "jazz-tools";
const note = co.plainText().create("");
</script>

<textarea
  value={note.toString()}
  oninput={e => {
    note.$jazz.applyDiff((e.target as HTMLTextAreaElement).value)
  }}
></textarea>
