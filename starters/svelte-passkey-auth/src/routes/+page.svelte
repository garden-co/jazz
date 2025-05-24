<script lang="ts">
import Form from "$lib/components/Form.svelte";
import { AccountCoState } from "jazz-svelte";

const account = new AccountCoState({
  resolve: {
    profile: true,
  },
});

$inspect(account);
</script>

<div class="container">
  <h1>Welcome back, {account?.current?.profile?.name}</h1>

  <Form />
</div>
