<script lang="ts">
    import { JazzProvider } from '$lib/jazz.svelte.js';
    import "jazz-inspector-element"
    import { TestAccount } from './schema.js';

    if (typeof window !== 'undefined') {
        localStorage.clear(); // Want to start always from a fresh account
    }
  
    let { children } = $props();
  </script>
  
  <JazzProvider
    AccountSchema={TestAccount}
    sync={{
      when: "never"
    }}
  >
    <jazz-inspector></jazz-inspector>
    {@render children()}
  </JazzProvider>
  