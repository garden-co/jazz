<script lang="ts">
  import { AccountCoState, CoState } from 'jazz-tools/svelte';
  import { TestAccount, Person } from './schema.js';

  const me = new AccountCoState(TestAccount, {
    resolve: {
      root: {
        people: true
      }
    }
  });

  let id = $state<string>();
  let customId = $state<string>();
  let currentBranch = $state<string>('main');
  let newBranchName = $state<string>('');

  const person = new CoState(Person, () => id,  () => ({
    resolve: { dog: true },
    unstable_branch: currentBranch === 'main' ? undefined : { name: currentBranch }
  }));

  // Function to create a new branch
  function createBranch() {
    if (newBranchName) {
      currentBranch = newBranchName;
      newBranchName = '';
    }
  }

  // Function to switch to main branch
  function switchToMain() {
    currentBranch = 'main';
  }

  // Function to merge current branch into main
  function mergeBranch() {
    if (person.current && currentBranch !== 'main') {
      person.current.$jazz.unstable_merge();
      // Switch back to main after merge
      currentBranch = 'main';
    }
  }
</script>

<div>
  <!-- Person Selection -->
  <div class="section">
    <h3>Person Selection</h3>
    {#each me.current?.root?.people?.$jazz.refs || [] as ref, index}
      <button
        onclick={() => {
          id = ref.id;
        }}>Select person [{index}]</button
      >
    {/each}
    <button
      onclick={() => {
        console.log(person.current?.name);
      }}>Log person</button
    >
    <button
      onclick={() => {
        id = undefined;
      }}>Set undefined</button
    >
    <button
      onclick={() => {
        id = customId;
      }}>Set customId</button
    >

    <label>
      Custom ID
      <input type="text" bind:value={customId} />
    </label>
  </div>

  <!-- Branch Management -->
  <div class="section">
    <h3>Branch Management</h3>
    <div class="branch-controls">
      <div>
        <button onclick={switchToMain}>Switch to Main</button>
      </div>

      <div>
        <label for="new-branch">Create new branch:</label>
        <input 
          id="new-branch" 
          type="text" 
          bind:value={newBranchName} 
          placeholder="Branch name"
          onkeydown={(e) => e.key === 'Enter' && createBranch()}
        />
        <button onclick={createBranch} disabled={!newBranchName}>
          Create Branch
        </button>
      </div>

      <div>
        <button 
          onclick={mergeBranch} 
          disabled={currentBranch === 'main' || !person.current}
          class="merge-button"
        >
          Merge to Main
        </button>
      </div>
    </div>
  </div>

  <div class="section">
    <button
      onclick={() => {
        me.logOut();
      }}>Logout</button
    >
  </div>
</div>

{#if person.current}
  <div class="section">
    <h3>Person Details</h3>
    <div class="person-info">
      <div class="branch-status" data-testid="branch-status">
        <strong>Branch Status:</strong>
        {person.current.$jazz.isBranch ? `Branch: ${person.current.$jazz.branchName}` : 'Main branch'}
      </div>
      
      <div class="person-fields">
        <label>
          Name
          <input type="text" bind:value={
            () => person.current!.name,
            newValue => person.current!.$jazz.set("name", newValue)
          } />
        </label>
        <label>
          Dog
          <input type="text" bind:value={
            () => person.current!.dog.name,
            newValue => person.current!.dog.$jazz.set("name", newValue)
          } />
        </label>
      </div>
      
      <div class="display-values">
        <div data-testid="person-name">
          <strong>Name:</strong> {person.current.name}
        </div>
        <div data-testid="person-dog-name">
          <strong>Dog:</strong> {person.current.dog.name}
        </div>
      </div>
    </div>
  </div>
{/if}

<style>
  .section {
    margin: 20px 0;
    padding: 15px;
    border: 1px solid #e0e0e0;
    border-radius: 6px;
    background-color: #fafafa;
  }

  .section h3 {
    margin: 0 0 15px 0;
    color: #333;
    font-size: 18px;
  }

  .branch-controls {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }

  .branch-controls > div {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }

  .branch-controls label {
    font-weight: 500;
    color: #555;
  }

  .branch-controls input {
    padding: 6px 10px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 14px;
  }

  .branch-controls input:focus {
    outline: none;
    border-color: #007acc;
    box-shadow: 0 0 0 2px rgba(0, 122, 204, 0.2);
  }

  .person-info {
    display: flex;
    flex-direction: column;
    gap: 15px;
  }

  .branch-status {
    padding: 10px;
    background-color: #f0f8ff;
    border: 1px solid #b3d9ff;
    border-radius: 4px;
    font-family: monospace;
    font-size: 14px;
  }

  .person-fields {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .person-fields label {
    display: flex;
    flex-direction: column;
    gap: 5px;
    font-weight: 500;
    color: #555;
  }

  .person-fields input {
    padding: 8px 12px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 14px;
  }

  .person-fields input:focus {
    outline: none;
    border-color: #007acc;
    box-shadow: 0 0 0 2px rgba(0, 122, 204, 0.2);
  }

  .display-values {
    padding: 10px;
    background-color: #f8f9fa;
    border: 1px solid #e9ecef;
    border-radius: 4px;
  }

  .display-values div {
    margin: 5px 0;
    font-family: monospace;
    font-size: 14px;
  }

  button {
    padding: 8px 16px;
    border: 1px solid #007acc;
    background-color: white;
    color: #007acc;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    font-weight: 500;
    transition: all 0.2s ease;
  }

  button:hover {
    background-color: #007acc;
    color: white;
  }

  button:disabled {
    background-color: #f5f5f5;
    border-color: #ddd;
    color: #999;
    cursor: not-allowed;
  }

  button:disabled:hover {
    background-color: #f5f5f5;
    color: #999;
  }

  .merge-button {
    background-color: #28a745;
    border-color: #28a745;
    color: white;
  }

  .merge-button:hover:not(:disabled) {
    background-color: #218838;
    border-color: #1e7e34;
  }
</style>
