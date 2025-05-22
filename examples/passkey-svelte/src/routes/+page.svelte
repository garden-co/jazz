<script lang="ts">
  import { AccountCoState } from 'jazz-svelte';
  import { Account } from 'jazz-tools';

  const account = new AccountCoState(Account, {
    resolve: {
      profile: true
    }
  });

  $inspect(account);
</script>

<div class="container">
  <h1>Welcome back, {account?.current?.profile?.name}</h1>
  <button onclick={() => account.logOut()}>Log out</button>
</div>

<style>
  .container {
    padding: 2rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;
    align-items: start;
  }

  button {
    background: #000;
    color: #fff;
    padding: 6px 12px;
    border: none;
    border-radius: 6px;
    min-height: 38px;
    cursor: pointer;
  }
</style>
