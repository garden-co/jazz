<script lang="ts">
  import { AccountCoState, InviteListener } from "jazz-tools/svelte";
  import { JazzAccount, Organization } from "./schema";
  const me = new AccountCoState(JazzAccount, {
    resolve: {
      root: {
        organizations: true
      }
    }
  });
  new InviteListener({
    invitedObjectSchema: Organization,
    onAccept: async (organizationID) => {
      console.log("Accepted invite!")
      const organization = await Organization.load(organizationID);
      if (!organization.$isLoaded || !me.current.$isLoaded)
        throw new Error("Error loading user or organization");
      me.current.root.organizations.$jazz.push(organization);
      // navigate to the organization page
    },
  });
</script>
