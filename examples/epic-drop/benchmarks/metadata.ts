import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/epic-drop/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database (test storage)";
const fileFixture = (size: string) =>
  `One folder holding one ${size} file, streamed in from a deterministic 32 KiB-per-read source.`;

const upload = (name: string, mib: number): BenchmarkMetadata => ({
  name,
  title: `EpicDrop upload · ${mib} MiB file`,
  description: `Stream one ${mib} MiB file into a folder as a large value and wait until it is locally durable.`,
  fixture: "An empty folder in a freshly opened database; a new database per measured upload.",
  storage,
  includes: [
    "Reading the source in 32 KiB windows",
    "Chunking and storing the large value",
    "Inserting the file's metadata row",
    "Waiting for local durability",
  ],
  excludes: ["Schema compilation and database opening", "Folder creation", "Browser File reads"],
  work: {
    count: mib * 1024 * 1024,
    unit: "bytes uploaded/s",
    explanation: `${mib} MiB of file contents per upload.`,
  },
  source,
});

export const epicDropBenchmarks: BenchmarkMetadata[] = [
  upload("epic_drop_upload_4mb", 4),
  upload("epic_drop_upload_64mb", 64),
  {
    name: "epic_drop_folder_listing_100_files",
    title: "EpicDrop folder listing · 100 files",
    description:
      "Read a folder's file list (id, name, type and size), ordered by name, as the browser view does. Only metadata columns are projected, but today the cost still grows with the size of the files in the folder (#3471), so this number should fall once that is fixed.",
    fixture: "One folder holding 100 streamed files of 256 KiB each.",
    storage,
    includes: ["Prepared metadata query over the folder", "Materializing 100 metadata rows"],
    excludes: ["Uploading the files", "Query preparation", "Rendering"],
    work: { count: 1, unit: "listings/s", explanation: "One full folder listing of 100 rows." },
    source,
  },
  {
    name: "epic_drop_download_4mb",
    title: "EpicDrop download · 4 MiB file",
    description: "Read a whole 4 MiB file back from storage, as the download button does.",
    fixture: fileFixture("4 MiB"),
    storage,
    includes: ["Resolving the file's large-value reference", "Reading and joining every chunk"],
    excludes: ["Uploading the file", "Writing the download to disk"],
    work: {
      count: 4 * 1024 * 1024,
      unit: "bytes downloaded/s",
      explanation: "4 MiB per download.",
    },
    source,
  },
  {
    name: "epic_drop_seek_64mb",
    title: "EpicDrop seek · 64 KiB from a 64 MiB file",
    description:
      "Read a 64 KiB window from the middle of a 64 MiB file, as an audio or video player does when the user scrubs. Today the cost grows with the whole file's size rather than the window's (#3471), so this number should fall once that is fixed.",
    fixture: fileFixture("64 MiB"),
    storage,
    includes: ["Resolving the file's large-value reference", "Reading the requested 64 KiB window"],
    excludes: ["Uploading the file", "Decoding the media"],
    work: { count: 1, unit: "seeks/s", explanation: "One 64 KiB range read per seek." },
    source,
  },
];
