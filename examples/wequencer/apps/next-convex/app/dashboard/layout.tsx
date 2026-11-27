import { AppFrame } from "@/components/app-frame";

// middleware.ts sends signed-out visitors to the sign-in page.
export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  return <AppFrame>{children}</AppFrame>;
}
