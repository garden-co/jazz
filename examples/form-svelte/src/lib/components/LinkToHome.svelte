<script lang="ts">
  import { goto } from '$app/navigation';

  function navigateHome() {
    goto('#/');
  }
</script>

<button 
  onclick={navigateHome}
  class="mb-4 text-blue-500 hover:text-blue-700 underline"
>
  ← Back to orders
</button> 
