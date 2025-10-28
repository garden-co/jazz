import { co, FileStream, Group, ID } from "jazz-tools";
const myGroup = Group.create();
const progressBar: HTMLElement = document.querySelector(".progress-bar")!;
import { createJazzTestAccount } from "jazz-tools/testing";
const colleagueAccount = await createJazzTestAccount();

// #region FromBlob
// From a file input
const fileInput = document.querySelector(
  'input[type="file"]',
) as HTMLInputElement;

fileInput.addEventListener("change", async () => {
  const file = fileInput.files?.[0];
  if (!file) return;

  // Create FileStream from user-selected file
  const fileStream = await co
    .fileStream()
    .createFromBlob(file, { owner: myGroup });

  // Or with progress tracking for better UX
  const fileWithProgress = await co.fileStream().createFromBlob(file, {
    onProgress: (progress) => {
      // progress is a value between 0 and 1
      const percent = Math.round(progress * 100);
      console.log(`Upload progress: ${percent}%`);
      progressBar.style.width = `${percent}%`;
    },
    owner: myGroup,
  });
});
// #endregion

// #region Empty
const fileStream = co.fileStream().create({ owner: myGroup });
// #endregion

// #region Ownership
// Create a team group
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

// Create a FileStream with shared ownership
const teamFileStream = co.fileStream().create({ owner: teamGroup });
// #endregion

// #region GetRaw
// Get all chunks and metadata
const fileData = fileStream.getChunks();

if (fileData) {
  console.log(`MIME type: ${fileData.mimeType}`);
  console.log(`Total size: ${fileData.totalSizeBytes} bytes`);
  console.log(`File name: ${fileData.fileName}`);
  console.log(`Is complete: ${fileData.finished}`);

  // Access raw binary chunks
  for (const chunk of fileData.chunks) {
    // Each chunk is a Uint8Array
    console.log(`Chunk size: ${chunk.length} bytes`);
  }
}
// #endregion

// #region GetIncomplete
// Get data even if the stream isn't complete
const partialData = fileStream.getChunks({ allowUnfinished: true });
// #endregion

// #region GetAsBlob
// Convert to a Blob
const blob = fileStream.toBlob();

// Get the filename from the metadata
const filename = fileStream.getChunks()?.fileName;

if (blob) {
  // Use with URL.createObjectURL
  const url = URL.createObjectURL(blob);

  // Create a download link
  const link = document.createElement("a");
  link.href = url;
  link.download = filename || "document.pdf";
  link.click();

  // Clean up when done
  URL.revokeObjectURL(url);
}
// #endregion

const fileStreamId = "co_z123" as ID<FileStream>;

// #region LoadAsBlob
// Load directly as a Blob when you have an ID
const blobFromID = await co.fileStream().loadAsBlob(fileStreamId);

// By default, waits for complete uploads
// For in-progress uploads:
const partialBlob = await co.fileStream().loadAsBlob(fileStreamId, {
  allowUnfinished: true,
});
// #endregion

// #region CheckSync
if (fileStream.isBinaryStreamEnded()) {
  console.log("File is completely synced");
} else {
  console.log("File upload is still in progress");
}
// #endregion

// #region ManualStart
// Create an empty FileStream
const manualFileStream = co.fileStream().create({ owner: myGroup });

// Initialize with metadata
manualFileStream.start({
  mimeType: "application/pdf", // MIME type (required)
  totalSizeBytes: 1024 * 1024 * 2, // Size in bytes (if known)
  fileName: "document.pdf", // Original filename (optional)
});
// #endregion

const file = [0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64]; // "Hello World" in ASCII
const bytes = new Uint8Array(file);
const arrayBuffer = bytes.buffer;

// #region AddChunks
const data = new Uint8Array(arrayBuffer);

// For large files, break into chunks (e.g., 100KB each)
const chunkSize = 1024 * 100;
for (let i = 0; i < data.length; i += chunkSize) {
  // Create a slice of the data
  const chunk = data.slice(i, i + chunkSize);

  // Push chunk to the FileStream
  fileStream.push(chunk);

  // Track progress
  const progress = Math.min(
    100,
    Math.round(((i + chunk.length) * 100) / data.length),
  );
  console.log(`Upload progress: ${progress}%`);
}

// #region Finalise
// Finalise the upload
fileStream.end();

console.log("Upload complete!");
// #endregion

// #region LoadById
// @ts-expect-error Options is required by the type at the moment, but not really necessary.
const fileStreamFromId = await co.fileStream().load(fileStreamId);

if (fileStream.$isLoaded) {
  console.log("FileStream loaded successfully");

  // Check if it's complete
  if (fileStream.isBinaryStreamEnded()) {
    // Process the completed file
    const blob = fileStream.toBlob();
  }
}
// #endregion

// #region SubscribeById
const unsubscribe = co
  .fileStream()
  .subscribe(fileStreamId, (fileStream: FileStream) => {
    // Called whenever the FileStream changes
    console.log("FileStream updated");

    // Get current status
    const chunks = fileStream.getChunks({ allowUnfinished: true });
    if (chunks) {
      const uploadedBytes = chunks.chunks.reduce(
        (sum: number, chunk: Uint8Array) => sum + chunk.length,
        0,
      );
      const totalBytes = chunks.totalSizeBytes || 1;
      const progress = Math.min(
        100,
        Math.round((uploadedBytes * 100) / totalBytes),
      );

      console.log(`Upload progress: ${progress}%`);

      if (fileStream.isBinaryStreamEnded()) {
        console.log("Upload complete!");
        // Now safe to use the file
        const blob = fileStream.toBlob();

        // Clean up the subscription if we're done
        unsubscribe();
      }
    }
  });
// #endregion

// #region WaitForSync
// Wait for the FileStream to be fully synced
await fileStream.$jazz.waitForSync({
  timeout: 5000, // Optional timeout in ms
});

console.log("FileStream is now synced to all connected devices");
// #endregion
