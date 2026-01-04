<script lang="ts">
  import { SignedIn, SignedOut, SignInButton, SignOutButton } from "svelte-clerk";
</script>

<SignedIn>
  <SignOutButton />
</SignedIn>
<SignedOut>
  <SignInButton />
</SignedOut>
