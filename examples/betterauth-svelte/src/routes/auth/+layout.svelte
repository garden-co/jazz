<script lang="ts">
  import jazzLogo from "$lib/assets/jazz.svg";
  const { children } = $props();
</script>

<div
  class="bg-muted flex min-h-svh flex-col items-center justify-center gap-6 p-6 md:p-10 w-full"
>
  <div class="flex w-full max-w-sm flex-col gap-6">
    <a href="/" class="flex items-center gap-2 self-center font-medium">
      <img src={jazzLogo} alt="Jazz Logo" width={24} height={24} />
      Jazz BetterAuth Demo
    </a>
    {@render children?.()}
  </div>
</div>
