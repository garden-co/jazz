<script lang="ts">
	import type { Db } from '../../../src/runtime/db.js';
	import { getSession } from '../../../src/svelte/context.svelte.js';

	let { db }: { db: Db } = $props();

	// Mirror the example pattern that captures the initial session value.
	const session = getSession();
	const sessionUserId = $derived(session?.user_id ?? 'missing');
</script>

<div data-testid="db-ready">{db ? 'yes' : 'no'}</div>
<div data-testid="session-user">{sessionUserId}</div>
