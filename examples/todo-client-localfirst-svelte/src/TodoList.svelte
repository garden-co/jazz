<script lang="ts">
	import { getDb, getSession, QuerySubscription } from 'jazz-tools/svelte';
	import { toast } from 'svelte-sonner';
	import { app } from '../schema.js';

	// #region reading-reactive-svelte
	const db = getDb();
	const todos = new QuerySubscription(app.todos);
	// #endregion reading-reactive-svelte
	const session = getSession();
	const sessionUserId = $derived(session?.user_id ?? null);
	let title = $state('');

	function handleSubmit(e: SubmitEvent) {
		e.preventDefault();
		if (!title.trim() || !sessionUserId) return;
		db.insert(app.todos, { title: title.trim(), done: false, owner_id: sessionUserId });
		title = '';
	}

	function toggleTodo(todo: { id: string; done: boolean }, event: Event) {
		const checkbox = event.currentTarget as HTMLInputElement;

		try {
			db.update(app.todos, todo.id, { done: !todo.done });
		} catch {
			checkbox.checked = todo.done;
			toast.error("You don't have permission to update this task");
		}
	}

	function deleteTodo(todoId: string) {
		try {
			db.delete(app.todos, todoId);
		} catch {
			toast.error("You don't have permission to delete this task");
		}
	}
</script>

<form onsubmit={handleSubmit}>
	<input
		type="text"
		bind:value={title}
		placeholder="What needs to be done?"
		required
	/>
	<button type="submit" disabled={!sessionUserId}>Add</button>
</form>
<ul id="todo-list">
	{#each todos.current ?? [] as todo (todo.id)}
			<li class={todo.done ? 'done' : ''}>
				<input
					type="checkbox"
					checked={todo.done}
					onchange={(event) => toggleTodo(todo, event)}
					class="toggle"
				/>
			<span>{todo.title}</span>
			{#if todo.description}
				<small>{todo.description}</small>
			{/if}
				<button class="delete-btn" onclick={() => deleteTodo(todo.id)}>
					&times;
				</button>
			</li>
	{/each}
</ul>
