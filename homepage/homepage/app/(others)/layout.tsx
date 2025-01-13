import { JazzNav } from "@/components/nav";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex-1 w-full">
      <JazzNav />
      <main>{children}</main>
    </div>
  );
}
