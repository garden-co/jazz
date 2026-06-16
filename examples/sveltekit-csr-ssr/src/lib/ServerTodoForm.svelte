<script lang="ts">
  import { enhance } from "$app/forms";
</script>

<form method="POST" action="?/addServer" use:enhance class="flex gap-2">
  <input
    name="titleField"
    type="text"
    placeholder="New todo…"
    class="flex-1 text-sm bg-transparent border border-foreground/15 rounded px-3 py-1.5 outline-none focus:border-foreground/40 placeholder:text-foreground/25"
  />
  <button
    type="submit"
    class="text-sm px-3 py-1.5 border border-foreground/15 rounded hover:bg-foreground/5 transition-colors cursor-pointer"
  >
    Add
  </button>
</form>
