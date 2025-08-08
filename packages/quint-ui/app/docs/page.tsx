import Link from "next/link";

export default function DocsPage() {
  return (
    <div>
      <h1 className="text-2xl">Components</h1>
      <ul className="my-4 text-lg font-semibold">
        <li>
          <Link href="/docs/button">Button</Link>
          <Link href="/docs/icon">Icon</Link>
        </li>
      </ul>
    </div>
  );
}
