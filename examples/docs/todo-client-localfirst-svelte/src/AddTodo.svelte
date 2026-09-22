<script lang="ts">
  import { getDb } from 'jazz-tools/svelte';
  import { app } from '../schema.js';

  const db = getDb();
  let title = $state('');
</script>

<form onsubmit={(e) => {
  e.preventDefault();
  db.insert(app.todos, { title, done: false });
  title = '';
}}>
  <input type="text" bind:value={title} placeholder="What needs to be done?" />
  <button type="submit">Add</button>
</form>
