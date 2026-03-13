<!-- #region auth-session-svelte -->
<script lang="ts">
	import { getDb, getSession } from 'jazz-tools/svelte';
	import { app } from '../schema/session-app.js';

	const db = getDb();
	const session = getSession();

	async function loadOwnedTodos() {
		if (!session) return [];
		return db.all(app.todos.where({ owner_id: session.user_id }));
	}

	function addOwnedTodo(title: string) {
		if (!session) return;

		db.insert(app.todos, {
			title,
			done: false,
			owner_id: session.user_id,
		});
	}
</script>
<!-- #endregion auth-session-svelte -->
