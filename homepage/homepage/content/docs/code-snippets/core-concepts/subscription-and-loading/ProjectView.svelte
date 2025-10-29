<script lang="ts">
  // [!code hide]
  import { Project } from './schema';
  import { CoState } from "jazz-tools/svelte";

  const { projectId } : { projectId: string } = $props();

  // Subscribe to a project and resolve its tasks
  const project = new CoState(Project, projectId, {
    resolve: { tasks: { $each: true } } // Tell Jazz to load each task in the list
  });
</script>

{#if !project.current.$isLoaded}
  {@const loadingState = project.current.$jazz.loadingState}
  {#if loadingState === "unauthorized"}
    Project not accessible
  {:else if loadingState === "unavailable"}
    Project not found
  {:else if loadingState === "loading"}
    Loading project...
  {/if}
{:else}
  <div>
    <h1>{project.current.name}</h1>
    <ul>
      {#each project.current.tasks as task (task.$jazz.id)}
        <li>{task.title}</li>
      {/each}
    </ul>
  </div>
{/if}
