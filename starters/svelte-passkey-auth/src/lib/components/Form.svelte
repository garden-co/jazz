<script lang="ts">
import { AccountCoState } from "jazz-svelte";

const me = new AccountCoState({
  resolve: {
    profile: true,
    root: true,
  },
});

$inspect(me.current);
</script>

<form>hello</form>
<!--<div class="grid gap-4 border p-8">-->
<!--  <div class="flex items-center gap-3">-->
<!--    <label for="firstName" class="sm:w-32">-->
<!--      First name-->
<!--    </label>-->
<!--    <input-->
<!--      type="text"-->
<!--      id="firstName"-->
<!--      placeholder="Enter your first name here..."-->
<!--      class="border border-stone-300 rounded shadow-sm py-1 px-2 flex-1"-->
<!--      bind:value={me.current.profile.firstName}-->
<!--    />-->
<!--  </div>-->

<!--  <div class="flex items-center gap-3">-->
<!--    <label for="dateOfBirth" class="sm:w-32">-->
<!--      Date of birth-->
<!--    </label>-->
<!--    <input-->
<!--      type="date"-->
<!--      id="dateOfBirth"-->
<!--      class="border border-stone-300 rounded shadow-sm py-1 px-2 flex-1"-->
<!--      value={me.current.root.dateOfBirth?.toISOString().split("T")[0] || ""}-->
<!--      onchange={(e) => (me.current.root.dateOfBirth = new Date(e.target?.value))}-->
<!--    />-->
<!--  </div>-->

<!--  &lt;!&ndash;Add more fields here&ndash;&gt;-->
<!--</div>-->
