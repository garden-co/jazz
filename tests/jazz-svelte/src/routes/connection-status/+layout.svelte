<script lang="ts">
    import { JazzSvelteProvider } from 'jazz-tools/svelte';
    import "jazz-tools/inspector/register-custom-element"

    if (typeof window !== 'undefined') {
        localStorage.clear(); // Want to start always from a fresh account
    }
  
    let { children } = $props();
  </script>
  
  <JazzSvelteProvider
    sync={{
      peer: "wss://cloud.jazz.tools/?key=your-api-key"
    }}
  >
    <jazz-inspector></jazz-inspector>
    {@render children()}
  </JazzSvelteProvider>
  