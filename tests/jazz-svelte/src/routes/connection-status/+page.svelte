<script lang="ts">
  import { SyncConnectionStatus } from "jazz-tools/svelte";

  const connectionStatus = new SyncConnectionStatus();
</script>

<div>
  <h1>Connection Status</h1>
  <p data-testid="connected">{connectionStatus.current ? "true" : "false"}</p>
</div>
