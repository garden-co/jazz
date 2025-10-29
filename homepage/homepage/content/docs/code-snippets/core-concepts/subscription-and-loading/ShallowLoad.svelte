<script lang="ts">
  import { CoState } from "jazz-tools/svelte";
  import { Project } from "./schema";

  const { projectId } : { projectId: string } = $props();

  const projectWithTasksShallow = new CoState(Project, projectId, {
    resolve: {
      tasks: true
    }
  });
</script>
