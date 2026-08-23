<script lang="ts">
  import type { Db } from "jazz-tools";
  import { getToken } from "$lib/auth-client";

  let { db }: { db: Db } = $props();

  $effect(() =>
    db.onAuthChanged(async (state) => {
      if (state.error !== "expired") return;
      const token = await getToken();
      if (token) db.updateAuthToken(token);
    }),
  );
</script>
