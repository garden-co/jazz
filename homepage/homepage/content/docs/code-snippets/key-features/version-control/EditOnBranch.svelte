<script lang="ts">
  import { CoState } from 'jazz-tools/svelte';
  import { Project } from './schema.js';

  let projectId = $state<string>();
  let currentBranchName = $state<string>('main');

  const project = new CoState(Project, () => projectId, () => ({
    resolve: {
      tasks: { $each: true }
    },
    unstable_branch: currentBranchName === 'main' ? undefined : { name: currentBranchName }
  }));
</script>

<form>
  {#if project.current.$isLoaded}
    <input
      type="text"
      bind:value={
        () => (project.current.$isLoaded && project.current.title) || "",
        (v) =>
          project.current.$isLoaded && project.current.$jazz.set("title", v)
      }
    />

    {#each project.current.tasks as task (task.$jazz.id)}
      <input
        type="text"
        bind:value={
          () => task.title || "",
          (v) => task.$isLoaded && task.$jazz.set("title", v)
        }
      />
    {/each}
  {/if}
</form>
