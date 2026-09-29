import * as React from "react";
import { useDb } from "jazz-tools/react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { CodeBlock } from "@astryxdesign/core/CodeBlock";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { Spinner } from "@astryxdesign/core/Spinner";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import {
  formatBytes,
  hexDump,
  previewKind,
  readFileBlob,
  readFileRange,
  saveBlob,
} from "../large-values.js";

export interface PreviewFile {
  id: string;
  name: string;
  content_type: string;
  size_bytes: number;
  modified: Date | null;
  owner: string;
}

/** Media below this size loads as soon as the preview opens. */
const AUTO_LOAD_BYTES = 32 * 1024 * 1024;
/** Text previews read the file one range at a time. */
export const TEXT_PAGE_BYTES = 64 * 1024;
/** Binary previews show the first bytes as hex. */
export const HEX_PREVIEW_BYTES = 512;

export function PreviewPanel({
  file,
  headingLevel = 2,
}: {
  file: PreviewFile;
  headingLevel?: 2 | 3;
}) {
  const db = useDb();
  const kind = previewKind(file.content_type);
  return (
    <VStack gap={4}>
      <Heading level={headingLevel} maxLines={2}>
        {file.name}
      </Heading>
      {kind === "text" ? (
        <TextPreview key={file.id} file={file} />
      ) : kind === "binary" ? (
        <HexPreview key={file.id} file={file} />
      ) : (
        <MediaPreview key={file.id} file={file} kind={kind} />
      )}
      <MetadataList>
        <MetadataListItem label="Type">{file.content_type}</MetadataListItem>
        <MetadataListItem label="Size">{formatBytes(file.size_bytes)}</MetadataListItem>
        {file.modified && (
          <MetadataListItem label="Modified">
            <Timestamp value={file.modified.getTime()} format="date_time" />
          </MetadataListItem>
        )}
        <MetadataListItem label="Owner">{file.owner}</MetadataListItem>
      </MetadataList>
      <HStack gap={2}>
        <Button
          label="Download"
          variant="primary"
          clickAction={async () => {
            saveBlob(await readFileBlob(db, file.id, file.content_type), file.name);
          }}
        />
      </HStack>
    </VStack>
  );
}

type LoadState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "ready"; url: string }
  | { status: "error"; message: string };

/**
 * Images, audio, video and PDFs render from a Blob URL. The browser seeks
 * inside that Blob, so the whole value is read once rather than per seek.
 */
function MediaPreview({
  file,
  kind,
}: {
  file: PreviewFile;
  kind: "image" | "audio" | "video" | "pdf";
}) {
  const db = useDb();
  const [state, setState] = React.useState<LoadState>({ status: "idle" });
  const load = React.useCallback(() => {
    setState({ status: "loading" });
    readFileBlob(db, file.id, file.content_type).then(
      (blob) => setState({ status: "ready", url: URL.createObjectURL(blob) }),
      (error: Error) => setState({ status: "error", message: error.message }),
    );
  }, [db, file.id, file.content_type]);

  React.useEffect(() => {
    if (file.size_bytes <= AUTO_LOAD_BYTES) load();
  }, [file.size_bytes, load]);
  React.useEffect(
    () => () => {
      if (state.status === "ready") URL.revokeObjectURL(state.url);
    },
    [state],
  );

  switch (state.status) {
    case "idle":
      return (
        <VStack gap={2} hAlign="start">
          <Text color="secondary">
            This file is {formatBytes(file.size_bytes)}. Previews of large files load on request.
          </Text>
          <Button label="Load preview" onClick={load} />
        </VStack>
      );
    case "loading":
      return <Spinner label="Loading preview" />;
    case "error":
      return <Banner status="error" title="Preview unavailable" description={state.message} />;
    case "ready":
      if (kind === "image")
        return <img className="preview-media" src={state.url} alt={file.name} />;
      if (kind === "audio") return <audio className="preview-media" src={state.url} controls />;
      if (kind === "video") return <video className="preview-media" src={state.url} controls />;
      return (
        <iframe className="preview-media preview-document" src={state.url} title={file.name} />
      );
  }
}

/** Text files are read one 64 KB range at a time, and only as far as you ask. */
function TextPreview({ file }: { file: PreviewFile }) {
  const db = useDb();
  const { id, size_bytes } = file;
  const [text, setText] = React.useState("");
  const [loadedBytes, setLoadedBytes] = React.useState(0);
  const [isLoading, setIsLoading] = React.useState(true);
  const [error, setError] = React.useState<string>();
  const decoder = React.useRef(new TextDecoder());

  const loadNext = React.useCallback(
    async (from: number, reset: boolean) => {
      setIsLoading(true);
      try {
        const bytes = await readFileRange(db, { id, size_bytes }, from, from + TEXT_PAGE_BYTES);
        const end = from + bytes.byteLength;
        if (reset) decoder.current = new TextDecoder();
        // `stream` keeps a code point split across two ranges intact.
        const chunk = decoder.current.decode(bytes, { stream: end < size_bytes });
        setText((current) => (reset ? chunk : current + chunk));
        setLoadedBytes(end);
      } catch (cause) {
        setError((cause as Error).message);
      } finally {
        setIsLoading(false);
      }
    },
    [db, id, size_bytes],
  );

  React.useEffect(() => {
    void loadNext(0, true);
  }, [loadNext]);

  if (error) return <Banner status="error" title="Preview unavailable" description={error} />;
  const isPartial = loadedBytes < file.size_bytes;
  return (
    <VStack gap={2}>
      <CodeBlock code={text} isWrapped maxHeight="60vh" size="sm" />
      {isPartial && (
        <HStack gap={3} vAlign="center" wrap="wrap">
          <Text type="supporting" color="secondary">
            Showing the first {formatBytes(loadedBytes)} of {formatBytes(file.size_bytes)}
          </Text>
          <Button
            label="Show more"
            size="sm"
            isLoading={isLoading}
            onClick={() => void loadNext(loadedBytes, false)}
          />
        </HStack>
      )}
    </VStack>
  );
}

/** Anything else shows its first bytes, which is often enough to recognise a format. */
function HexPreview({ file }: { file: PreviewFile }) {
  const db = useDb();
  const { id, size_bytes } = file;
  const [dump, setDump] = React.useState<string>();
  const [error, setError] = React.useState<string>();
  React.useEffect(() => {
    let cancelled = false;
    readFileRange(db, { id, size_bytes }, 0, HEX_PREVIEW_BYTES).then(
      (bytes) => !cancelled && setDump(hexDump(bytes)),
      (cause: Error) => !cancelled && setError(cause.message),
    );
    return () => {
      cancelled = true;
    };
  }, [db, id, size_bytes]);
  if (error) return <Banner status="error" title="Preview unavailable" description={error} />;
  if (dump === undefined) return <Spinner label="Reading the first bytes" />;
  return (
    <VStack gap={2}>
      <Text type="supporting" color="secondary">
        No preview for this type. The first{" "}
        {formatBytes(Math.min(HEX_PREVIEW_BYTES, file.size_bytes))} are shown below.
      </Text>
      <CodeBlock code={dump} size="sm" maxHeight="40vh" />
    </VStack>
  );
}
