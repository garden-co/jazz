import Link from 'next/link';

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col items-center justify-center text-center px-4">
      <h1 className="text-4xl font-bold mb-4">Jazz Documentation</h1>
      <p className="text-lg text-muted-foreground mb-8 max-w-2xl">
        Jazz is a distributed database that syncs across frontend, backend, and
        cloud. Build real-time, collaborative applications with ease.
      </p>
      <Link
        href="/docs"
        className="inline-flex items-center justify-center rounded-md bg-primary px-6 py-3 text-sm font-medium text-primary-foreground shadow transition-colors hover:bg-primary/90"
      >
        Get Started
      </Link>
    </main>
  );
}
