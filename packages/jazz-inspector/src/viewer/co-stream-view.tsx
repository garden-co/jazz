import {
  CoID,
  LocalNode,
  RawBinaryCoStream,
  RawCoStream,
  RawCoValue,
} from "cojson";
import { base64URLtoBytes } from "cojson";
import { BinaryStreamItem, BinaryStreamStart, CoStreamItem } from "cojson";
import type { JsonObject, JsonValue } from "cojson";
import { useEffect, useState } from "react";
import { Button } from "../ui/button.js";
import { PageInfo } from "./types.js";
import { AccountOrGroupPreview } from "./value-renderer.js";

import { classNames } from "../utils.js";

// typeguard for BinaryStreamStart
function isBinaryStreamStart(item: unknown): item is BinaryStreamStart {
  return (
    typeof item === "object" &&
    item !== null &&
    "type" in item &&
    item.type === "start"
  );
}

export function detectCoStreamType(value: RawCoStream | RawBinaryCoStream) {
  const firstKey = Object.keys(value.items)[0];
  if (!firstKey)
    return {
      type: "unknown",
    };

  const items = value.items[firstKey as never]?.map((v) => v.value);

  if (!items)
    return {
      type: "unknown",
    };
  const firstItem = items[0];
  if (!firstItem)
    return {
      type: "unknown",
    };
  // This is a binary stream
  if (isBinaryStreamStart(firstItem)) {
    return {
      type: "binary",
      items: items as BinaryStreamItem[],
    };
  } else {
    return {
      type: "coStream",
    };
  }
}

async function getBlobFromCoStream({
  items,
  onlyFirstChunk = false,
}: {
  items: BinaryStreamItem[];
  onlyFirstChunk?: boolean;
}) {
  if (onlyFirstChunk && items.length > 1) {
    items = items.slice(0, 2);
  }

  const chunks: Uint8Array[] = [];

  const binary_U_prefixLength = 8;

  let lastProgressUpdate = Date.now();

  for (const item of items.slice(1)) {
    if (item.type === "end") {
      break;
    }

    if (item.type !== "chunk") {
      console.error("Invalid binary stream chunk", item);
      return undefined;
    }

    const chunk = base64URLtoBytes(item.chunk.slice(binary_U_prefixLength));
    // totalLength += chunk.length;
    chunks.push(chunk);

    if (Date.now() - lastProgressUpdate > 100) {
      lastProgressUpdate = Date.now();
    }
  }
  const defaultMime = "mimeType" in items[0] ? items[0].mimeType : null;

  const blob = new Blob(chunks, defaultMime ? { type: defaultMime } : {});

  const mimeType =
    defaultMime === "" ? await detectPDFMimeType(blob) : defaultMime;

  return {
    blob,
    mimeType: mimeType as string,
    unfinishedChunks: items.length > 1,
    totalSize:
      "totalSizeBytes" in items[0]
        ? (items[0].totalSizeBytes as number)
        : undefined,
  };
}

const detectPDFMimeType = async (blob: Blob): Promise<string> => {
  const arrayBuffer = await blob.slice(0, 4).arrayBuffer();
  const uint8Array = new Uint8Array(arrayBuffer);
  const header = uint8Array.reduce(
    (acc, byte) => acc + String.fromCharCode(byte),
    "",
  );

  if (header === "%PDF") {
    return "application/pdf";
  }
  return "application/octet-stream";
};

const BinaryDownloadButton = ({
  pdfBlob,
  fileName = "document",
  label,
  mimeType,
}: {
  pdfBlob: Blob;
  mimeType?: string;
  fileName?: string;
  label: string;
}) => {
  const downloadFile = () => {
    const url = URL.createObjectURL(
      new Blob([pdfBlob], mimeType ? { type: mimeType } : {}),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download =
      mimeType === "application/pdf" ? `${fileName}.pdf` : fileName;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <Button variant="secondary" onClick={downloadFile}>
      ⬇️ {label}
      {/* Download {mimeType === "application/pdf" ? "PDF" : "File"} */}
    </Button>
  );
};

const LabelContentPair = ({
  label,
  content,
}: {
  label: string;
  content: React.ReactNode;
}) => {
  return (
    <div className={classNames("flex flex-col gap-1.5")}>
      <span>{label}</span>
      <span>{content}</span>
    </div>
  );
};

function RenderCoBinaryStream({
  value,
  items,
}: {
  items: BinaryStreamItem[];
  value: RawBinaryCoStream;
}) {
  const [file, setFile] = useState<
    | {
        blob: Blob;
        mimeType: string;
        unfinishedChunks: boolean;
        totalSize: number | undefined;
      }
    | undefined
    | null
  >(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // load only the first chunk to get the mime type and size
    getBlobFromCoStream({
      items,
      onlyFirstChunk: true,
    })
      .then((v) => {
        if (v) {
          setFile(v);
          if (v.mimeType.includes("image")) {
            // If it's an image, load the full blob
            getBlobFromCoStream({
              items,
            }).then((s) => {
              if (s) setFile(s);
            });
          }
        }
      })
      .finally(() => setIsLoading(false));
  }, [items]);

  if (!isLoading && !file) return <div>No blob</div>;

  if (isLoading) return <div>Loading...</div>;
  if (!file) return <div>No blob</div>;

  const { blob, mimeType } = file;

  const sizeInKB = (file.totalSize || 0) / 1024;

  return (
    <div className={classNames("mt-8 flex flex-col gap-8")}>
      <div className={classNames("grid grid-cols-3 gap-2 max-w-3xl")}>
        <LabelContentPair
          label="Mime Type"
          content={
            <span
              className={classNames(
                "font-mono bg-gray-100 rounded px-2 py-1 text-sm dark:bg-stone-900",
              )}
            >
              {mimeType || "No mime type"}
            </span>
          }
        />
        <LabelContentPair
          label="Size"
          content={<span>{sizeInKB.toFixed(2)} KB</span>}
        />
        <LabelContentPair
          label="Download"
          content={
            <BinaryDownloadButton
              fileName={value.id.toString()}
              pdfBlob={blob}
              mimeType={mimeType}
              label={
                mimeType === "application/pdf"
                  ? "Download PDF"
                  : "Download file"
              }
            />
          }
        />
      </div>
      {mimeType === "image/png" || mimeType === "image/jpeg" ? (
        <LabelContentPair
          label="Preview"
          content={
            <div
              className={classNames("bg-gray-50  dark:bg-gray-925 p-3 rounded")}
            >
              <RenderBlobImage blob={blob} />
            </div>
          }
        />
      ) : null}
    </div>
  );
}

function RenderCoStream({
  value,
  node,
}: {
  value: RawCoStream;
  node: LocalNode;
}) {
  const streamPerUser = Object.keys(value.items);
  const userCoIds = streamPerUser.map((stream) => stream.split("_session")[0]);

  return (
    <div className={classNames("grid grid-cols-3 gap-2")}>
      {userCoIds.map((id, idx) => (
        <div
          className={classNames(
            "p-3 rounded-lg overflow-hidden  border border-gray-200 cursor-pointer shadow-sm hover:bg-gray-100/5",
          )}
          key={id}
        >
          <AccountOrGroupPreview coId={id as CoID<RawCoValue>} node={node} />
          {/* @ts-expect-error - TODO: fix types */}
          {value.items[streamPerUser[idx]]?.map(
            (item: CoStreamItem<JsonValue>) => (
              <div>
                {new Date(item.madeAt).toLocaleString()}{" "}
                {JSON.stringify(item.value)}
              </div>
            ),
          )}
        </div>
      ))}
    </div>
  );
}

export function CoStreamView({
  value,
  node,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
  value: RawCoStream;
}) {
  // if (!value) return <div>No value</div>;

  const streamType = detectCoStreamType(value);

  if (streamType.type === "binary") {
    if (streamType.items === undefined) {
      return <div>No binary stream</div>;
    }

    return (
      <RenderCoBinaryStream
        value={value as RawBinaryCoStream}
        items={streamType.items}
      />
    );
  }

  if (streamType.type === "coStream") {
    return <RenderCoStream value={value} node={node} />;
  }

  if (streamType.type === "unknown") return <div>Unknown stream type</div>;

  return <div>Unknown stream type</div>;
}

function RenderBlobImage({ blob }: { blob: Blob }) {
  const urlCreator = window.URL || window.webkitURL;
  return <img src={urlCreator.createObjectURL(blob)} />;
}
