<!-- #region read-write-svelte -->
<script lang="ts">
	import { getDb, QuerySubscription } from 'jazz-tools/svelte';
	import { app } from '../schema/app.js';

	// #region writing-get-db-svelte
	// #region reading-reactive-svelte
	const db = getDb();
	const todos = new QuerySubscription(app.todos);
	// #endregion reading-reactive-svelte
	// #endregion writing-get-db-svelte

	// #region filtering-svelte
	const incompleteTodos = new QuerySubscription(
		app.todos.where({ done: false }).orderBy('title', 'asc').limit(50),
	);
	// #endregion filtering-svelte

	// #region reading-tier-svelte
	const confirmedTodos = new QuerySubscription(app.todos, { tier: 'edge' });
	// #endregion reading-tier-svelte

	let title = $state('');

	async function handleSubmit(e: SubmitEvent) {
		e.preventDefault();
		if (!title.trim()) return;
		// #region writing-insert-svelte
		await db.insert(app.todos, { title: title.trim(), done: false });
		// #endregion writing-insert-svelte
		title = '';
	}

	// #region writing-mutations-svelte
	async function toggleTodo(todo: { id: string; done: boolean }) {
		await db.update(app.todos, todo.id, { done: !todo.done });
	}

	async function removeTodo(id: string) {
		await db.deleteFrom(app.todos, id);
	}
	// #endregion writing-mutations-svelte

	// #region writing-durability-svelte
	async function addImportantTodo(todoTitle: string) {
		const { id } = await db.insert(app.todos, { title: todoTitle, done: false }, { tier: 'edge' });
		await db.update(app.todos, id, { done: true }, { tier: 'edge' });
		await db.deleteFrom(app.todos, id, { tier: 'global' });
	}
	// #endregion writing-durability-svelte
</script>

<form onsubmit={handleSubmit}>
	<input type="text" bind:value={title} placeholder="What needs to be done?" required />
	<button type="submit">Add</button>
</form>
<!-- #region render-list-svelte -->
<ul id="todo-list">
	{#each todos.current ?? [] as todo (todo.id)}
		<li class={todo.done ? 'done' : ''}>
			<input
				type="checkbox"
				checked={todo.done}
				onchange={() => toggleTodo(todo)}
				class="toggle"
			/>
			<span>{todo.title}</span>
			{#if todo.description}
				<small>{todo.description}</small>
			{/if}
			<button class="delete-btn" onclick={() => removeTodo(todo.id)}>
				&times;
			</button>
		</li>
	{/each}
</ul>
<!-- #endregion render-list-svelte -->
<!-- #endregion read-write-svelte -->
