"use client";

import { SettingsForm } from "jazz-react-auth-betterauth";

export default function SettingsPage() {
  return <SettingsForm.default providers={["github"]} />;
}
