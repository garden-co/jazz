import { Providers } from "./providers";

export const metadata = { title: "jazz-eval-a64-next" };

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "sans-serif", padding: 16 }}>
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
