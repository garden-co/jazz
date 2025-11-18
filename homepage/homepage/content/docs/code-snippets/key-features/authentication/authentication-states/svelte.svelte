<script lang="ts">
  import { AccountCoState } from 'jazz-tools/svelte';
  import { MusicaAccount } from './schema';
  const account = new AccountCoState(MusicaAccount);
  const isAuthenticated = $derived(account.isAuthenticated);
  const agent = $derived(account.agent);
</script>
