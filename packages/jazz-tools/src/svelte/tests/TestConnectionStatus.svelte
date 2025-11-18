<script>
  import { SyncConnectionStatus } from "../jazz.class.svelte";
  const connectionStatus = new SyncConnectionStatus();
</script>

<div>
  <div data-testid="connected">{connectionStatus.current ? "true" : "false"}</div>
</div>
