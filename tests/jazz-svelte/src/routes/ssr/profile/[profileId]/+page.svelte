<script lang="ts">
  import type { PageProps } from "./$types.js";

  let { data }: PageProps = $props();
</script>

<p data-testid="ssr-profile-name">{data.profile.name}</p>
<a href="/ssr" data-testid="back">&larr; Back</a>
