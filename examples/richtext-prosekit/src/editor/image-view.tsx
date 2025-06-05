import { UploadTask } from "prosekit/extensions/file";
import type { ImageAttrs } from "prosekit/extensions/image";
import type { ReactNodeViewProps } from "prosekit/react";
import { ResizableHandle, ResizableRoot } from "prosekit/react/resizable";
import { type SyntheticEvent, useEffect, useState } from "react";

export default function ImageView(props: ReactNodeViewProps) {
  const { setAttrs, node } = props;
  const attrs = node.attrs as ImageAttrs;
  const url = attrs.src || "";
  const uploading = url.startsWith("blob:");

  const [aspectRatio, setAspectRatio] = useState<number | undefined>();
  const [error, setError] = useState<string | undefined>();
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    if (!url.startsWith("blob:")) {
      return;
    }

    const uploadTask = UploadTask.get<string>(url);
    if (!uploadTask) {
      return;
    }

    const abortController = new AbortController();
    void uploadTask.finished
      .then((resultUrl) => {
        if (resultUrl && typeof resultUrl === "string") {
          if (abortController.signal.aborted) {
            return;
          }
          setAttrs({ src: resultUrl });
        } else {
          if (abortController.signal.aborted) {
            return;
          }
          setError("Unexpected upload result");
        }
        UploadTask.delete(uploadTask.objectURL);
      })
      .catch((error) => {
        if (abortController.signal.aborted) {
          return;
        }
        setError(String(error));
        UploadTask.delete(uploadTask.objectURL);
      });
    const unsubscribe = uploadTask.subscribeProgress(({ loaded, total }) => {
      if (abortController.signal.aborted) {
        return;
      }
      if (total > 0) {
        setProgress(loaded / total);
      }
    });
    return () => {
      unsubscribe();
      abortController.abort();
    };
  }, [url, setAttrs]);

  const handleImageLoad = (event: SyntheticEvent) => {
    const img = event.target as HTMLImageElement;
    const { naturalWidth, naturalHeight } = img;
    const ratio = naturalWidth / naturalHeight;
    if (ratio && Number.isFinite(ratio)) {
      setAspectRatio(ratio);
    }
    if (naturalWidth && naturalHeight && (!attrs.width || !attrs.height)) {
      setAttrs({ width: naturalWidth, height: naturalHeight });
    }
  };

  return (
    <ResizableRoot
      width={attrs.width ?? undefined}
      height={attrs.height ?? undefined}
      aspectRatio={aspectRatio}
      onResizeEnd={(event) => setAttrs(event.detail)}
      data-selected={props.selected ? "" : undefined}
      className="relative flex items-center justify-center box-border overflow-hidden my-2 group max-h-[600px] max-w-full min-h-[64px] min-w-[64px] outline-2 outline-transparent data-[selected]:outline-blue-500 outline-solid"
    >
      {url && !error && (
        <img
          src={url}
          onLoad={handleImageLoad}
          className="h-full w-full max-w-full max-h-full object-contain"
        />
      )}
      {uploading && !error && (
        <div className="absolute bottom-0 left-0 m-1 flex content-center items-center gap-2 rounded bg-gray-800/60 p-1.5 text-xs text-white/80 transition">
          <div className="i-lucide-loader-circle h-4 w-4 animate-spin"></div>
          <div>{Math.round(progress * 100)}%</div>
        </div>
      )}
      {error && (
        <div className="absolute bottom-0 left-0 right-0 top-0 flex flex-col items-center justify-center gap-4 bg-gray-200 p-2 text-sm dark:bg-gray-800 @container">
          <div className="i-lucide-image-off h-8 w-8"></div>
          <div className="hidden opacity-80 @xs:block">
            Failed to upload image
          </div>
        </div>
      )}
      <ResizableHandle
        className="absolute bottom-0 right-0 rounded m-1.5 p-1 transition bg-gray-900/30 active:bg-gray-800/60 hover:bg-gray-800/60 text-white/50 active:text-white/80 active:translate-x-0.5 active:translate-y-0.5 opacity-0 hover:opacity-100 group-hover:opacity-100 group-[[data-resizing]]:opacity-100"
        position="bottom-right"
      >
        <div className="i-lucide-arrow-down-right h-4 w-4"></div>
      </ResizableHandle>
    </ResizableRoot>
  );
}
