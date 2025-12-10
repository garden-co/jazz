<script lang="ts">
  import { AccountCoState } from "jazz-tools/svelte";
  import { MyAccount } from "./schema";

  const me = new AccountCoState(MyAccount, {
    resolve: { root: true },
    unstable_branch: { name: "feature-branch" },
  });

  me.current.$isLoaded && me.current.$jazz.set("root", { value: "Feature Branch" }); // Will also modify the main account
  me.current.$isLoaded && me.current.root.$jazz.set("value", "Feature Branch"); // This only modifies the branch
</script>
