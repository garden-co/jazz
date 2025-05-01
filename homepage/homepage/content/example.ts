export type Example = {
  name: string;
  slug: string;
  description?: string;
  illustration?: React.ReactNode;
  tech?: string[];
  features?: string[];
  demoUrl?: string;
  showDemo?: boolean;
  imageUrl?: string;
  codeSamples?: { name: string; content: React.ReactNode }[];
};

export const tech = {
  react: "React",
  reactNative: "React Native",
  expo: "Expo",
  vue: "Vue",
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
