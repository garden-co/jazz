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

  function handleTitleChange(e: Event) {
    const target = e.target as HTMLInputElement;
    // Won't be visible on main until merged
    project.current.$isLoaded && project.current.$jazz.set("title", target.value);
  }

  function handleTaskTitleChange(index: number, e: Event) {
    const target = e.target as HTMLInputElement;
    const task = project.current.$isLoaded && project.current?.tasks[index];

    // The task is also part of the branch because we used the `resolve` option
    // with `tasks: { $each: true }`
    // so the changes won't be visible on main until merged
    task && task.$jazz.set("title", target.value);
  }
</script>

<form>
  <!-- Edit form fields -->
  {#if project.current.$isLoaded}
  <input
    type="text"
    value={project.current.title || ''}
    oninput={handleTitleChange}
  />

  {#each project.current.tasks as task, index}
    <input
      type="text"
      value={task.title || ''}
      oninput={(e) => handleTaskTitleChange(index, e)}
    />
  {/each}
  {/if}
</form>
