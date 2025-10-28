import { UserSettings } from "@/components/user-settings";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Settings | Jazz Example: Better Auth",
};

export default function SettingsPage() {
  return (
    <div className="max-w-screen-md w-full mx-auto px-4">
      <UserSettings />
    </div>
  );
}
