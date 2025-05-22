<script lang="ts">
  import { AccountCoState } from 'jazz-svelte';
  import { SharedFile, FileShareAccount } from '$lib/schema';
  import { FileStream, type Loaded } from 'jazz-tools';
  import FileItem from '$lib/components/FileItem.svelte';
  import { CloudUpload } from 'lucide-svelte';

  const me = new AccountCoState(FileShareAccount, {
    resolve: {
      profile: true,
      root: {
        sharedFiles: {
          $each: true
        }
      }
    }
  });

  $inspect(me);

  const sharedFiles = $derived(me.current?.root.sharedFiles);

  let fileInput: HTMLInputElement;

  async function handleFileUpload(event: Event) {
    const input = event.target as HTMLInputElement;
    const files = input.files;

    if (!files?.length || !sharedFiles) return;

    const file = files[0];
    const fileName = file.name;
    const createdAt = new Date();

    try {
      // Create a FileStream from the uploaded file
      const fileStream = await FileStream.createFromBlob(file, sharedFiles._owner);

      // Create the shared file entry
      const sharedFile = SharedFile.create(
        {
          name: fileName,
          file: fileStream,
          createdAt,
          uploadedAt: new Date(),
          size: file.size
        },
        sharedFiles._owner
      );

      // Add the file to the user's files list
      sharedFiles.push(sharedFile);
    } finally {
      fileInput.value = ''; // reset input
    }
  }

  async function deleteFile(file: Loaded<typeof SharedFile>) {
    if (!sharedFiles) return;

    const index = sharedFiles.indexOf(file);
    if (index > -1) {
      sharedFiles.splice(index, 1);
    }
  }
</script>

<div class="min-h-screen bg-gray-50">
  <div class="container mx-auto max-w-4xl px-4 py-8">
    <div class="mb-12 flex items-center justify-between">
      <div>
        <h1 class="mb-2 text-4xl font-bold text-gray-900">File Share</h1>
        <h2 class="text-xl text-gray-600">Welcome back, {me.current?.profile.name}</h2>
      </div>

      <button
        onclick={me.logOut}
        class="rounded-lg bg-red-500 px-6 py-2.5 text-sm font-medium text-white transition-colors hover:bg-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2"
      >
        Log Out
      </button>
    </div>

    <!-- Upload Section -->
    <div class="mb-8 rounded-xl bg-white p-6 shadow-sm">
      <div
        class="group relative flex cursor-pointer flex-col items-center justify-center rounded-lg border-2 border-dashed border-gray-300 bg-white p-12 text-center hover:border-blue-500 hover:bg-blue-50"
        onclick={() => fileInput.click()}
        onkeydown={(e) => e.key === 'Enter' && fileInput.click()}
        role="button"
        tabindex="0"
      >
        <CloudUpload class="mb-2 h-8 w-8 text-gray-400 group-hover:text-blue-600" />
        <h3 class="mb-1 text-lg font-medium text-gray-900">Upload a new file</h3>
        <p class="text-sm text-gray-500">Click to select a file from your computer</p>
        <input
          type="file"
          bind:this={fileInput}
          onchange={handleFileUpload}
          class="hidden"
          accept="*/*"
        />
      </div>
    </div>

    <!-- Files List -->
    <div class="space-y-4">
      {#if sharedFiles}
        {#if sharedFiles.length}
          {#each sharedFiles as file}
            {#if file}
              <FileItem {file} onDelete={deleteFile} />
            {/if}
          {/each}
        {:else}
          <p class="text-center text-gray-500">No files yet</p>
        {/if}
      {/if}
    </div>
  </div>
</div>
