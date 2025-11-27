import { JazzWrapper } from "@/app/components/JazzWrapper";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <JazzWrapper>{children}</JazzWrapper>
      </body>
    </html>
  );
}
