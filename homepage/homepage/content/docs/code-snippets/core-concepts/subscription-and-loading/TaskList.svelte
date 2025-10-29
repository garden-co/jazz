<script lang="ts">
  import { co } from "jazz-tools";
  import { Project } from "./schema";
  type ProjectWithTasks = co.loaded<typeof Project,
    {
      tasks: {
        $each: true
      };
    }
  >;

  const { project } : { project: ProjectWithTasks } = $props();
</script>

<!-- In case the project prop isn't loaded as required, TypeScript will warn -->
<!-- TypeScript knows tasks are loaded, so this is type-safe -->
<ul>
  {#each project.tasks as task (task.$jazz.id)}
    <li>{task.title}</li>
  {/each}
</ul>
