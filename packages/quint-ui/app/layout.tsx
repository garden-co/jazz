import "@/src/globals.css";
import Link from "next/link";

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <title>Quint UI</title>
      </head>
      <body>
        <div className="flex gap-4 text-sm bg-stone-100 dark:bg-stone-800 p-2 px-5 rounded-md font-semibold">
          <Link href="/">Colors</Link>
          <Link href="/docs/button">Button</Link>
          <Link href="/docs/icons">Icons</Link>
        </div>
        <div className="container m-21">{children}</div>
      </body>
    </html>
  );
}
