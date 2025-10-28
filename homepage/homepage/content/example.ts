export type Example = {
  name: string;
  slug: string;
  description?: string;
  illustration?: React.ReactNode;
  tech?: string[];
  features?: string[];
  demoUrl?: string;
  imageUrl?: string;
  codeSamples?: { name: string; content: React.ReactNode }[];
  starter?: boolean;
};

export const tech = {
  react: "React",
  reactNative: "React Native",
  expo: "Expo",
  svelte: "Svelte",
};

export const features = {
  fileUpload: "File upload",
  imageUpload: "Image upload",
  passkey: "Passkey auth",
  clerk: "Clerk auth",
  inviteLink: "Invite link",
  coFeed: "CoFeed",
  coRichText: "CoRichText",
  coPlainText: "CoPlainText",
  serverWorker: "Server worker",
  inbox: "Inbox",
};
