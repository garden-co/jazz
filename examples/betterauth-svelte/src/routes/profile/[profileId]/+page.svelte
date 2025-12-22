<script lang="ts">
  import type { PageProps } from "./$types";

  let { data }: PageProps = $props();
</script>

<h1>SSR rendering example with Jazz</h1>
<p>This component fetched data on the server.</p>
<p>Your profile name: {data.profile?.name || "No name found"}</p>
<a href="/" role="button">&larr; Back</a>
