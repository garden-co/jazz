import { JazzReactProvider } from "jazz-tools/react";

const apiKey = process.env.NEXT_PUBLIC_JAZZ_API_KEY;

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <JazzReactProvider
          sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
        >
          {children}
        </JazzReactProvider>
      </body>
    </html>
  );
}
