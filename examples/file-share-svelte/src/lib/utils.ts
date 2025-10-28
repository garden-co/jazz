/**
 * Formats a file size in bytes to a human readable string
 * @param bytes The size in bytes
 * @returns A formatted string like "1.5 MB"
 */
export function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
}

/**
 * Generates a temporary file ID based on the file name and creation time
 * @param fileName The name of the file
 * @param createdAt The creation date
 * @returns A unique file ID string
 */
export function generateTempFileId(fileName: string | undefined, createdAt: Date | undefined): string {
  return `file-${fileName ?? 'unknown'}-${createdAt?.getTime() ?? 0}`;
}

export function downloadFileBlob(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}