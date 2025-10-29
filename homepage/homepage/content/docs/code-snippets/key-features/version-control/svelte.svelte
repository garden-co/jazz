<script lang="ts">
import { CoState } from 'jazz-tools/svelte';
import { Project } from './schema'
const projectId = '';
// #region CoState
const branch = new CoState(Project, projectId, {
  unstable_branch: { name: "feature-branch" }
});
// #endregion
</script>
