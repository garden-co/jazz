import Link from "next/link";

export default function HomePage() {
  return (
    <div className="flex flex-col justify-center text-center flex-1">
      <h1 className="text-2xl font-bold mb-4">Jazz 2 Documentation</h1>
      <p>
        Open{" "}
        <Link href="/docs" className="font-medium underline">
          /docs
        </Link>{" "}
        to view setup, schema, permissions, migration, and data API guides.
      </p>
    </div>
  );
}
