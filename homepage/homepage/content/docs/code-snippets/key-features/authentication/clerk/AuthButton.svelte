<script lang="ts">
  // @ts-ignore svelte-clerk is not installed in the homepage project but it is in the monorepo
  import { SignedIn, SignedOut, SignInButton, SignOutButton } from "svelte-clerk";
</script>

<SignedIn>
  <SignOutButton />
</SignedIn>
<SignedOut>
  <SignInButton />
</SignedOut>
