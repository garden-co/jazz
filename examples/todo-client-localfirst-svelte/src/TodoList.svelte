<script lang="ts">
	import { getDb, getJazzContext, QuerySubscription } from 'jazz-tools/svelte';
	import { app } from '../schema/app.js';

	// #region reading-reactive-svelte
	const db = getDb();
	const todos = new QuerySubscription(app.todos);
	// #endregion reading-reactive-svelte
	const jazz = getJazzContext();
	const sessionUserId = $derived(jazz.session?.user_id ?? null);
	let title = $state('');

	function handleSubmit(e: SubmitEvent) {
		e.preventDefault();
		if (!title.trim() || !sessionUserId) return;
		db.insert(app.todos, { title: title.trim(), done: false, owner_id: sessionUserId });
		title = '';
	}
</script>

{#if sessionUserId}
	<form onsubmit={handleSubmit}>
		<input
			type="text"
			bind:value={title}
			placeholder="What needs to be done?"
			required
		/>
		<button type="submit">Add</button>
	</form>
	<ul id="todo-list">
		{#each todos.current ?? [] as todo (todo.id)}
			<li class={todo.done ? 'done' : ''}>
				<input
					type="checkbox"
					checked={todo.done}
					onchange={() => db.update(app.todos, todo.id, { done: !todo.done })}
					class="toggle"
				/>
				<span>{todo.title}</span>
				{#if todo.description}
					<small>{todo.description}</small>
				{/if}
				<button class="delete-btn" onclick={() => db.deleteFrom(app.todos, todo.id)}>
					&times;
				</button>
			</li>
		{/each}
	</ul>
{:else}
	<p>Loading session...</p>
{/if}
