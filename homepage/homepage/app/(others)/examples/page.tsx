import { ExampleCard } from "@/components/examples/ExampleCard";
import { ClerkFullLogo } from "@/components/icons/ClerkFullLogo";
import { ReactLogo } from "@/components/icons/ReactLogo";
import { ReactNativeLogo } from "@/components/icons/ReactNativeLogo";
import { SvelteLogo } from "@/components/icons/SvelteLogo";
import { VueLogo } from "@/components/icons/VueLogo";
import { Example, features, tech } from "@/content/example";
import { H2 } from "@garden-co/design-system/src/components/atoms/Headings";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { GappedGrid } from "@garden-co/design-system/src/components/molecules/GappedGrid";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import { clsx } from "clsx";
import type { Metadata } from "next";

const title = "Examples";
const description =
  "Find an example app with code most similar to what you want to build.";

export const metadata: Metadata = {
  title,
  description,
  openGraph: {
    title,
    description,
  },
};

const MockButton = ({ children }: { children: React.ReactNode }) => (
  <p className="bg-blue-100 text-blue-800 py-1 px-3 rounded-full font-medium  text-xs inline-flex items-center justify-center">
    {children}
  </p>
);

const ChatIllustration = () => (
  <div className="p-4 flex flex-col gap-4 justify-center h-full">
    <div className="flex flex-col gap-1 items-end">
      <p className="text-2xs">Sebastian</p>
      <p className="inline-block text-xs py-1.5 p-2 rounded-full bg-blue text-white">
        No one likes jazz. Not even you.
      </p>
    </div>

    <div className="flex flex-col gap-1 items-start">
      <p className="text-2xs">Mia</p>
      <p className="inline-block text-xs py-1.5 p-2 rounded-full bg-stone-200 text-stone-900 dark:bg-white">
        I do like jazz now, because of you.
      </p>
    </div>
  </div>
);

const ClerkIllustration = () => (
  <div className="flex items-center justify-center h-full p-8">
    <ClerkFullLogo className="w-36 h-auto" />
  </div>
);

const FormIllustration = () => (
  <div className="flex bg-stone-100 h-full flex-col items-center justify-center dark:bg-transparent">
    <div className="p-3 flex flex-col rounded-md shadow-xl shadow-stone-400/20 bg-white sm:p-5 dark:shadow-none">
      <div className="w-16 h-1 rounded-full bg-stone-400 mb-1.5" />
      <div className="w-40 h-5 rounded border mb-3 dark:border-stone-500" />
      <div className="w-16 h-1 rounded-full bg-stone-400 mb-1.5 hidden sm:block" />
      <div className="w-40 h-5 rounded border mb-3 hidden sm:block dark:border-stone-500" />

      <div className="flex items-center gap-2 mb-5">
        <div className="w-3 h-3 rounded border dark:border-stone-500" />
        <div className="w-16 h-1 rounded-full bg-stone-400" />
      </div>
      <MockButton>Submit</MockButton>
    </div>
  </div>
);

const OrganizationRow = ({
  name,
  members,
  bgClassName,
  children,
}: {
  name: string;
  members: number;
  bgClassName: string;
  children?: React.ReactNode;
}) => {
  return (
    <div className="flex items-center py-2 px-3">
      <span
        className={clsx(
          bgClassName,
          "size-8 rounded text-white font-medium text-lg inline-flex items-center justify-center mr-3",
        )}
      >
        {name[0]}
      </span>

      <div className="flex-1">
        <p className="text-sm font-medium text-stone-900 dark:text-white">
          {name}
        </p>
        <p className="text-xs text-stone-500 dark:text-stone-600">
          {members} {members > 1 ? "members" : "member"}
        </p>
      </div>

      {children}
    </div>
  );
};

const OrganizationIllustration = () => (
  <div className="flex bg-stone-100 h-full flex-col items-center justify-center dark:bg-transparent">
    <div className="mx-auto">
      <OrganizationRow
        name="Garden Computing"
        members={7}
        bgClassName="bg-green-600"
      >
        <Icon name="chevronDown" className="ml-6" />
      </OrganizationRow>

      <div className="mt-3 rounded border divide-y shadow-sm bg-white dark:bg-transparent">
        <OrganizationRow
          name="Friends"
          members={5}
          bgClassName="bg-orange-600"
        />
        <OrganizationRow
          name="Alice's projects"
          members={1}
          bgClassName="bg-rose-600"
        />
      </div>
    </div>
  </div>
);

const VersionHistoryIllustration = () => (
  <div className="flex flex-col justify-center items-center h-full">
    <ul className="inline-flex flex-col gap-2 max-w-96 md:gap-4">
      {[
        {
          userInitial: "B",
          color: "orange-600",
          user: "Bob",
          action: "changed status to",
          value: "Backlog",
        },
        {
          userInitial: "A",
          color: "rose-600",
          user: "Alice",
          action: "changed status to",
          value: "In progress",
        },
        {
          userInitial: "A",
          color: "rose-600",
          user: "Alice",
          action: "changed estimate to",
          value: "5",
        },
      ].map((change, index) => (
        <li
          key={index}
          className="flex items-center gap-2 text-sm sm:text-base md:gap-3"
        >
          <span
            className={`size-5 shrink-0 text-xs inline-flex items-center justify-center font-medium rounded-full text-white bg-${change.color} sm:size-6`}
          >
            {change.userInitial}
          </span>
          <p>
            {change.user} {change.action}{" "}
            <span className="font-medium text-stone-900 dark:text-white">
              {change.value}
            </span>
          </p>
        </li>
      ))}
    </ul>
  </div>
);

const MusicIllustration = () => (
  <div className="flex flex-col items-center justify-center h-full p-8">
    <div className="p-3 w-[12rem] h-[8rem] border border-dashed border-blue dark:border-blue-500 rounded-lg flex gap-2 flex-col items-center justify-center">
      <Icon
        name="upload"
        size="4xl"
        className="stroke-blue mx-auto dark:stroke-blue-500"
      />
      <p className="whitespace-nowrap text-stone-900 dark:text-white">
        take-five.mp3
      </p>
    </div>
  </div>
);

const JazzPaperScissorsIllustration = () => (
  <div className="flex flex-col items-center justify-center h-full p-8 text-4xl">
    ✊ ✋ ✌️
  </div>
);

const ImageUploadIllustration = () => (
  <div className="flex flex-col items-center justify-center h-full p-8">
    <div className="p-3 w-[12rem] h-[8rem] border border-dashed border-blue dark:border-blue-500 rounded-lg flex gap-2 flex-col items-center justify-center">
      <Icon
        name="upload"
        size="4xl"
        className="stroke-blue mx-auto dark:stroke-blue-500"
      />
      <p className="whitespace-nowrap text-stone-900 dark:text-white">
        profile-photo.jpg
      </p>
    </div>
  </div>
);

const FileUploadIllustration = () => (
  <div className="flex flex-col items-center justify-center h-full p-8">
    <div className="p-3 w-[12rem] h-[8rem] border border-dashed border-blue dark:border-blue-500 rounded-lg flex gap-2 flex-col items-center justify-center">
      <Icon
        name="upload"
        size="4xl"
        className="stroke-blue mx-auto dark:stroke-blue-500"
      />
      <p className="whitespace-nowrap text-stone-900 dark:text-white">
        take-five.mp3
      </p>
    </div>

    <div className=" w-[12rem] h-2 rounded-full overflow-hidden bg-stone-200 mt-3">
      <div className="w-3/4 h-full bg-green-500" />
    </div>
    <div className="w-[12rem] flex justify-between text-xs mt-1.5">
      <p>Uploading...</p>

      <p>76%</p>
    </div>
  </div>
);

const ReactionsIllustration = () => (
  <div className="flex bg-stone-100 h-full flex-col items-center justify-center dark:bg-transparent">
    <div className="inline-flex justify-center gap-1.5 mx-auto">
      {["😍", "😮", "🤩", "😂", "👍"].map((emoji) => (
        <button
          type="button"
          key={emoji}
          className="size-10 text-xl rounded shadow-sm bg-white leading-none"
        >
          {emoji}
        </button>
      ))}
    </div>
  </div>
);

const MultiCursorIllustration = () => (
  <div className="flex bg-stone-100 h-full flex-col items-center justify-center dark:bg-transparent p-4">
    <div className=" bg-white min-w-64 md:aspect-[3/2] flex flex-col rounded-md shadow-xl shadow-stone-400/20 dark:shadow-none">
      <div className="w-full py-2 flex items-center gap-1.5 px-2 border-b dark:border-b-stone-200">
        <span className="rounded-full size-2 bg-stone-200"></span>
        <span className="rounded-full size-2 bg-stone-200"></span>
        <span className="rounded-full size-2 bg-stone-200"></span>
      </div>

      <div className="h-full mx-auto flex flex-col justify-center p-12">
        <div className="inline-block relative px-1 ring-1 ring-blue-400">
          <div className="absolute size-2 bg-white border border-blue-400 -left-1 -top-1"></div>
          <div className="absolute size-2 bg-white border border-blue-400 -right-1 -top-1"></div>
          <div className="absolute size-2 bg-white border border-blue-400 -left-1 -bottom-1"></div>
          <div className="absolute size-2 bg-white border border-blue-400 -right-1 -bottom-1"></div>

          <span className="text-lg font-semibold md:text-2xl md:font-bold text-stone-800">
            Hello, world!
          </span>
          <div className="absolute -top-10 right-4 text-rose-600 flex items-end gap-1">
            <Icon name="cursor"></Icon> <span className="text-xs">Mia</span>
          </div>
          <div className="absolute -bottom-10 left-4 text-green-600 flex items-end gap-1">
            <Icon name="cursor"></Icon>{" "}
            <span className="text-xs">Sebastian</span>
          </div>
        </div>
      </div>
    </div>
  </div>
);

const CoTextIllustration = () => (
  <div className="flex bg-stone-100 h-full flex-col items-center justify-center dark:bg-transparent p-4">
    <div className=" bg-white md:aspect-[3/2] min-w-64 flex flex-col rounded-md shadow-xl shadow-stone-400/20 dark:shadow-none">
      <div className="flex gap-2 p-3 border-b">
        <Icon name="bold" size="xs" />
        <Icon name="italic" size="xs" />
        <Icon name="code" size="xs" />
      </div>
      <div className="py-2 px-3 text-xl text-stone-800">
        <em>Hello</em>, <strong>world!</strong>
      </div>
    </div>
  </div>
);

const PetIllustration = () => (
  <div className="h-full p-4 bg-[url('/dog.jpg')] bg-cover bg-center p-4 flex items-end">
    <div className="inline-flex justify-center gap-1 mx-auto">
      {["😍", "😮", "🤩", "😂", "👍"].map((emoji) => (
        <button
          type="button"
          key={emoji}
          className="size-6 rounded shadow-sm bg-white leading-none"
        >
          {emoji}
        </button>
      ))}
    </div>
  </div>
);

const PasswordManagerIllustration = () => (
  <div className="max-w-[30rem] mx-auto flex flex-col justify-center h-full p-5 gap-4">
    <div className="flex justify-between items-center">
      <p className="font-display font-medium tracking-tight text-sm text-stone-900 dark:text-white">
        Password manager
      </p>

      <button
        type="button"
        className="border py-1 p-2 rounded-full font-medium text-xs"
      >
        Log out
      </button>
    </div>

    <table className="text-xs">
      <thead>
        <tr className="w-full text-stone-700 bg-stone-50 border-b dark:bg-transparent dark:text-stone-400">
          <th className="font-medium tracking-wider text-left uppercase p-2">
            Username
          </th>
          <th className="font-medium tracking-wider text-left uppercase p-2">
            URI
          </th>
          <th className="font-medium tracking-wider text-left uppercase p-2">
            Actions
          </th>
        </tr>
      </thead>
      <tbody>
        {[
          {
            email: "user@gmail.com",
            domain: "gmail.com",
          },
          {
            email: "user@gmail.com",
            domain: "fb.com",
          },
          {
            email: "user@gmail.com",
            domain: "x.com",
          },
        ].map(({ email, domain }) => (
          <tr className="border-b max-sm:last:hidden" key={domain}>
            <td className="p-2">{email}</td>
            <td className="p-2">{domain}</td>
            <td className="p-2">
              <MockButton>
                <Icon name="copy" size="2xs" className="mr-1" />
                Password
              </MockButton>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

const FileShareIllustration = () => (
  <div className="flex flex-col items-center justify-center h-full gap-3 p-8">
    <p>This file was shared with you.</p>
    <div className="p-3 w-full border rounded-lg flex justify-between gap-5">
      <div className="flex items-center gap-2">
        <Icon
          name="zip"
          size="xl"
          className="stroke-blue dark:stroke-blue-500"
        />
        <p className="whitespace-nowrap text-stone-900 dark:text-white">
          top-secret.zip
        </p>
      </div>

      <MockButton>Download</MockButton>
    </div>
  </div>
);

const PasskeyIllustration = () => (
  <div className="flex bg-stone-100 h-full flex-col items-center justify-center dark:bg-transparent">
    <div className="p-4 flex flex-col items-center gap-3 rounded-md shadow-xl shadow-stone-400/20 bg-white dark:shadow-none">
      <Icon name="touchId" size="3xl" className="stroke-red-600" />
      <p className="text-xs dark:text-stone-900">Continue with Touch ID</p>
    </div>
  </div>
);

const reactExamples: Example[] = [
  {
    name: "Chat",
    slug: "chat",
    description: "A simple app that creates a chat room with a shareable link.",
    tech: [tech.react],
    demoUrl: "https://chat.jazz.tools",
    illustration: <ChatIllustration />,
  },
  {
    name: "Image upload",
    slug: "image-upload",
    description: "Learn how to upload and delete images.",
    tech: [tech.react],
    features: [features.imageUpload],
    demoUrl: "https://image-upload-demo.jazz.tools",
    illustration: <ImageUploadIllustration />,
  },
  {
    name: "File upload",
    slug: "filestream",
    description:
      "Upload different types of files, and show upload progress, file size, and type.",
    tech: [tech.react],
    features: [features.fileUpload],
    demoUrl: "https://file-upload-demo.jazz.tools",
    illustration: <FileUploadIllustration />,
  },
  {
    name: "Reactions",
    slug: "reactions",
    description: "Collect and render reactions from multiple users.",
    tech: [tech.react],
    features: [features.coFeed],
    demoUrl: "https://reactions-demo.jazz.tools",
    illustration: <ReactionsIllustration />,
  },
  {
    name: "Cursor presence",
    slug: "multi-cursors",
    description:
      "Track user presence on a canvas with multiple cursors and out of bounds indicators.",
    tech: [tech.react],
    features: [features.coFeed],
    demoUrl: "https://multi-cursors-demo.jazz.tools",
    illustration: <MultiCursorIllustration />,
  },
  {
    name: "Collaborative rich text",
    slug: "richtext",
    description:
      "Handle multiple users editing the same text, integrated with a ProseMirror editor for rich text.",
    tech: [tech.react],
    features: [features.coRichText, features.coPlainText],
    demoUrl: "https://richtext-demo.jazz.tools",
    illustration: <CoTextIllustration />,
  },
  {
    name: "Rate my pet",
    slug: "pets",
    description:
      "Upload a photo of your pet, and invite your friends to react to it.",
    tech: [tech.react],
    features: [features.imageUpload, features.inviteLink],
    demoUrl: "https://pets-demo.jazz.tools",
    illustration: <PetIllustration />,
  },
  {
    name: "Todo list",
    slug: "todo",
    description: "A todo list where you can collaborate with invited guests.",
    tech: [tech.react],
    features: [features.inviteLink],
    demoUrl: "https://todo-demo.jazz.tools",
    illustration: (
      <div className="h-full w-full bg-cover bg-[url('/todo.jpg')] bg-left-bottom"></div>
    ),
  },
  {
    name: "Password manager",
    slug: "password-manager",
    description: "A secure password manager, using Passkey for authentication.",
    tech: [tech.react],
    features: [features.passkey],
    demoUrl: "https://passwords-demo.jazz.tools",
    illustration: <PasswordManagerIllustration />,
  },
  {
    name: "Music player",
    slug: "music-player",
    description:
      "Upload your favorite songs, and share them with your friends.",
    tech: [tech.react],
    features: [features.fileUpload],
    demoUrl: "https://music-demo.jazz.tools",
    illustration: <MusicIllustration />,
  },
  // {
  //   name: "Jazz paper scissors",
  //   slug: "jazz-paper-scissors",
  //   description:
  //     "A game that shows how to communicate with other accounts through the experimental Inbox API.",
  //   tech: [tech.react],
  //   features: [features.serverWorker, features.inbox],
  //   illustration: <JazzPaperScissorsIllustration />,
  //   demoUrl: "https://jazz-paper-scissors.vercel.app",
  // },
  {
    name: "Clerk",
    slug: "clerk",
    description: "A React app that uses Clerk for authentication",
    tech: [tech.react],
    features: [features.clerk],
    demoUrl: "https://clerk-demo.jazz.tools",
    illustration: <ClerkIllustration />,
  },
  {
    name: "Passkey",
    slug: "passkey",
    description: "A React app that uses Passkey for authentication",
    tech: [tech.react],
    features: [features.passkey],
    demoUrl: "https://passkey-demo.jazz.tools",
    illustration: <PasskeyIllustration />,
  },
  {
    name: "Form",
    slug: "form",
    description: "A form example for creating and editing CoValues",
    tech: [tech.react],
    demoUrl: "https://form-demo.jazz.tools",
    illustration: <FormIllustration />,
  },
  {
    name: "Organization/Team",
    slug: "organization",
    description:
      "Collaborate with members of your organization through a shared CoMap acting as a root or main database",
    tech: [tech.react],
    features: [features.inviteLink],
    illustration: <OrganizationIllustration />,
    demoUrl: "https://jazz-organization.vercel.app",
  },
  {
    name: "Version history",
    slug: "version-history",
    description:
      "Track and restore previous versions of your data, and see who made the changes.",
    tech: [tech.react],
    illustration: <VersionHistoryIllustration />,
    demoUrl: "https://jazz-version-history.vercel.app",
  },
];

const rnExamples: Example[] = [
  {
    name: "Chat",
    slug: "chat-rn",
    description:
      "A simple React Native app that creates a chat room with a shareable link.",
    tech: [tech.reactNative],
    illustration: <ChatIllustration />,
  },

  {
    name: "Chat",
    slug: "chat-rn-expo",
    description:
      "A simple Expo app that creates a chat room with a shareable link.",
    tech: [tech.reactNative, tech.expo],
    illustration: <ChatIllustration />,
  },

  {
    name: "Chat",
    slug: "chat-rn-expo-clerk",
    description:
      "Exactly like the React Native Expo chat app, with Clerk for auth.",
    tech: [tech.reactNative, tech.expo],
    features: [features.clerk],
    illustration: <ClerkIllustration />,
  },
];

const vueExamples: Example[] = [
  {
    name: "Chat",
    slug: "chat-vue",
    description: "A simple app that creates a chat room with a shareable link.",
    tech: [tech.vue],
    illustration: <ChatIllustration />,
  },
  {
    name: "Todo list",
    slug: "todo-vue",
    description: "A todo list where you can collaborate with invited guests.",
    tech: [tech.vue],
    features: [features.inviteLink],
    illustration: (
      <div className="h-full w-full bg-cover bg-[url('/todo.jpg')] bg-left-bottom"></div>
    ),
  },
];

const svelteExamples: Example[] = [
  {
    name: "Passkey",
    slug: "passkey-svelte",
    description: "A Svelte app that uses Passkey for authentication",
    tech: [tech.svelte],
    features: [features.passkey],
    illustration: <PasskeyIllustration />,
  },
  {
    name: "File share",
    slug: "file-share-svelte",
    description: "Upload a file, then share the link for others to download.",
    tech: [tech.svelte],
    features: [features.fileUpload, features.passkey, features.inviteLink],
    illustration: <FileShareIllustration />,
  },
];

const categories = [
  {
    name: "React",
    id: "react",
    logo: ReactLogo,
    examples: reactExamples,
  },
  {
    name: "React Native",
    id: "react-native",
    logo: ReactNativeLogo,
    examples: rnExamples,
  },
  {
    name: "Vue",
    id: "vue",
    logo: VueLogo,
    examples: vueExamples,
  },
  {
    name: "Svelte",
    id: "svelte",
    logo: SvelteLogo,
    examples: svelteExamples,
  },
];

export default function Page() {
  return (
    <div className="container flex flex-col gap-6 pb-10 lg:pb-20">
      <HeroHeader
        title="Example apps"
        slogan="Find an example app with code most similar to what you want to build"
      />

      <div className="grid gap-12 lg:gap-20">
        {categories.map((category) => (
          <div key={category.name}>
            <div className="flex items-center gap-3 mb-5">
              <category.logo className="h-8 w-8" />
              <H2 id={category.id} className="!mb-0">
                {category.name}
              </H2>
            </div>

            <GappedGrid>
              {category.examples.map((example) => (
                <ExampleCard
                  className="border bg-stone-50 shadow-sm p-3 rounded-lg dark:bg-stone-950"
                  key={example.slug}
                  example={example}
                />
              ))}
            </GappedGrid>
          </div>
        ))}
      </div>
    </div>
  );
}
