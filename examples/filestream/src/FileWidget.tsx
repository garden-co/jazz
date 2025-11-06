"use client";
import { co } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import { useRef, useState } from "react";
import { JazzAccount } from "./schema";

export function FileWidget() {
  const inputRef = useRef<HTMLInputElement>(null);
  const dragAndDropElementRef = useRef<HTMLDivElement>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const me = useAccount(JazzAccount, { resolve: { profile: true } });

  if (!me.$isLoaded) {
    return (
      <div className="flex-1 flex justify-center items-center">Loading...</div>
    );
  }

  const profile = me.profile;

  async function handleUpload(event: React.FormEvent) {
    event.preventDefault();
    setError(null);

    setProgress(0);

    const file = inputRef.current?.files?.[0];
    if (!file) {
      setError("Please select a file");
      return;
    }

    try {
      setIsUploading(true);
      profile.$jazz.set(
        "file",
        await co.fileStream().createFromBlob(file, {
          onProgress: (p) => setProgress(Math.round(p * 100)),
        }),
      );
    } catch (error) {
      setError(
        error instanceof Error ? error.message : "Failed to upload file",
      );
      console.error("Error uploading file:", error);
    } finally {
      setIsUploading(false);
    }
  }

  function handleDelete() {
    profile.$jazz.delete("file");
  }

  async function handleDownload() {
    const file = profile.file;
    if (!file?.$isLoaded) return;

    try {
      const blob = file.toBlob();
      const url = URL.createObjectURL(blob || new Blob());
      const a = document.createElement("a");
      a.href = url;
      a.download = file.$jazz.id || "download";
      a.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error("Error downloading file:", error);
    }
  }

  function formatFileSize(bytes?: number): string {
    if (!bytes) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  }

  const onDragEnter = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    dragAndDropElementRef?.current?.classList.replace(
      "border-stone-400",
      "border-blue-700",
    );
  };

  const onDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    dragAndDropElementRef?.current?.classList.replace(
      "border-stone-400",
      "border-blue-700",
    );
  };

  const onDragLeave = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    dragAndDropElementRef?.current?.classList.replace(
      "border-blue-700",
      "border-stone-400",
    );
  };

  const onDrop = async (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    dragAndDropElementRef?.current?.classList.replace(
      "border-blue-700",
      "border-stone-400",
    );

    const droppedFiles = e.dataTransfer.files;
    if (!droppedFiles || droppedFiles.length === 0) {
      return;
    }

    const file = droppedFiles[0];
    setProgress(0);

    try {
      setIsUploading(true);
      profile.$jazz.set(
        "file",
        await co.fileStream().createFromBlob(file, {
          onProgress: (p) => setProgress(Math.round(p * 100)),
        }),
      );
    } catch (error) {
      setError(
        error instanceof Error ? error.message : "Failed to upload file",
      );
      console.error("Error uploading file:", error);
    } finally {
      setIsUploading(false);
    }
  };

  if (!profile.file?.$isLoaded) {
    return (
      <div className="flex flex-col gap-4">
        <div
          className="flex flex-col border-2 border-dashed border-stone-400 h-48 items-center justify-center bg-stone-100 m-5 rounded-md"
          ref={dragAndDropElementRef}
          onDragEnter={onDragEnter}
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          onDrop={onDrop}
        >
          <p>Drag & drop your file here</p>
        </div>
        <form onSubmit={handleUpload} className="flex gap-2">
          <input
            className="bg-stone-100 py-1.5 px-2 text-sm rounded-md w-4/5"
            ref={inputRef}
            type="file"
            accept="file"
            onChange={() => setError(null)}
            disabled={isUploading}
          />

          <button
            type="submit"
            className="bg-stone-100 py-1.5 px-3 text-sm rounded-md disabled:opacity-50"
            disabled={isUploading}
          >
            {isUploading ? `Uploading...` : "Upload file"}
          </button>
        </form>
        {error && (
          <div className="text-red-800 text-sm pl-4" role="alert">
            {error}
          </div>
        )}

        {isUploading && (
          <div className="flex gap-2 items-center">
            <div className="py-1.5 px-3 text-sm">Progress: {progress}%</div>
            <div className="h-2 bg-stone-200 rounded-full flex-1">
              <div
                className="h-full bg-blue-500 rounded-full transition-all duration-300"
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>
        )}
      </div>
    );
  }

  const fileData = profile.file.getMetadata();
  const mimeType = fileData?.mimeType || "unknown";

  return (
    <div className="gap-2">
      <div className="p-4">
        <div className="space-y-2">
          <div className="flex justify-between">
            <span className="font-bold">File name</span>
            <span>{fileData?.fileName}</span>
          </div>
          <div className="flex justify-between">
            <span className="font-bold">Type</span>
            <span>{mimeType}</span>
          </div>
          <div className="flex justify-between">
            <span className="font-bold">Size</span>
            <span>{formatFileSize(fileData?.totalSizeBytes)}</span>
          </div>
          <div className="flex justify-between">
            <span className="font-bold">CoValue ID</span>
            <span>{profile.file.$jazz.id}</span>
          </div>
        </div>
      </div>

      <div className="flex gap-2 justify-end">
        <button
          className="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
          onClick={handleDelete}
        >
          Delete file
        </button>

        <button
          className="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
          onClick={handleDownload}
        >
          Download file
        </button>
      </div>

      <div className="w-full justify-center pt-8">
        {mimeType.startsWith("image/") && (
          <div className="flex justify-center">
            <img
              src={URL.createObjectURL(profile.file.toBlob() || new Blob())}
              alt="File preview"
              className="max-w-xs mb-4"
            />
          </div>
        )}

        {mimeType.startsWith("audio/") && (
          <div className="flex justify-center">
            <audio
              src={URL.createObjectURL(profile.file.toBlob() || new Blob())}
              controls
              className="w-full mb-4"
            />
          </div>
        )}

        {mimeType.startsWith("video/") && (
          <div className="flex justify-center">
            <video
              src={URL.createObjectURL(profile.file.toBlob() || new Blob())}
              controls
              className="w-full mb-4"
            />
          </div>
        )}
      </div>
    </div>
  );
}
