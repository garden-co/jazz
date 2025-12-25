<script lang="ts">
  import { getJazzContext } from 'jazz-tools/svelte';
  const context = getJazzContext();

  function logIn(username: string, password: string) {
    // Whatever your existing auth system is
    // @ts-expect-error Virtual implementation
    const myOldAppUser = myApp.logIn(username, password);
    const accountID = myOldAppUser.jazzAccountID;
    // If you've stored this in an encrypted form, make sure to decrypt it first
    const accountSecret = myOldAppUser.jazzAccountSecret;
    context.current.authenticate({ accountID, accountSecret });
    // The Jazz session is now authenticated!
  }
</script>
