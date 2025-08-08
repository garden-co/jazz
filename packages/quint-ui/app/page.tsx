import { BookIcon } from "lucide-react";
import Link from "next/link";
import { Button } from "@/src/components/button";

export default function Home() {
  return (
    <main className="flex flex-col gap-8">
      <h1 className="text-3xl font-bold tracking-tight">Quint UI</h1>

      {/* Example usage of the button component with an icon and rendered as a nextjs link */}
      <Button
        variant="default"
        intent="primary"
        size="lg"
        render={<Link href="/docs" />}
      >
        <BookIcon />
        Read the docs!
      </Button>
    </main>
  );
}
