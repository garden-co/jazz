import { Button } from "@/components/ui/button";
import { betterAuthClient } from "@/lib/auth-client";
import {
  SiApple,
  SiDiscord,
  SiDropbox,
  SiFacebook,
  SiGithub,
  SiGitlab,
  SiGoogle,
  SiHuggingface,
  SiKick,
  SiLinear,
  SiNotion,
  SiReddit,
  SiRoblox,
  SiSlack,
  SiSpotify,
  SiTiktok,
  SiTwitch,
  SiVk,
  SiX,
  SiZoom,
} from "@icons-pack/react-simple-icons";
import type { SocialProviderList } from "better-auth/social-providers";
import { useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { toast } from "sonner";

interface SocialProvider {
  name: string;
  icon?: ReactNode;
}

const socialProviderMap: Partial<
  Record<SocialProviderList[number], SocialProvider>
> = {
  github: {
    name: "GitHub",
    icon: <SiGithub />,
  },
  google: {
    name: "Google",
    icon: <SiGoogle />,
  },
  apple: {
    name: "Apple",
    icon: <SiApple />,
  },
  discord: {
    name: "Discord",
    icon: <SiDiscord />,
  },
  facebook: {
    name: "Facebook",
    icon: <SiFacebook />,
  },
  microsoft: {
    name: "Microsoft",
  },
  twitter: {
    name: "X",
    icon: <SiX />,
  },
  dropbox: {
    name: "Dropbox",
    icon: <SiDropbox />,
  },
  linkedin: {
    name: "LinkedIn",
  },
  gitlab: {
    name: "GitLab",
    icon: <SiGitlab />,
  },
  kick: {
    name: "Kick",
    icon: <SiKick />,
  },
  tiktok: {
    name: "TikTok",
    icon: <SiTiktok />,
  },
  twitch: {
    name: "Twitch",
    icon: <SiTwitch />,
  },
  vk: {
    name: "VK",
    icon: <SiVk />,
  },
  zoom: {
    name: "Zoom",
    icon: <SiZoom />,
  },
  roblox: {
    name: "Roblox",
    icon: <SiRoblox />,
  },
  reddit: {
    name: "Reddit",
    icon: <SiReddit />,
  },
  spotify: {
    name: "Spotify",
    icon: <SiSpotify />,
  },
  slack: {
    name: "Slack",
    icon: <SiSlack />,
  },
  linear: {
    name: "Linear",
    icon: <SiLinear />,
  },
  notion: {
    name: "Notion",
    icon: <SiNotion />,
  },
  huggingface: {
    name: "Hugging Face",
    icon: <SiHuggingface />,
  },
};

interface Props {
  provider: SocialProviderList[number];
  link?: boolean;
}

export function SSOButton({ provider, link = false }: Props) {
  const router = useRouter();

  if (!socialProviderMap[provider]) {
    return <div>Provider not supported</div>;
  }

  return (
    <Button
      type="button"
      variant="outline"
      onClick={() => {
        if (link) {
          betterAuthClient.linkSocial({ provider });
        } else {
          betterAuthClient.signIn.social({ provider });
        }
      }}
    >
      {socialProviderMap[provider].icon}
      {link ? "Link" : "Continue with"} {socialProviderMap[provider].name}
    </Button>
  );
}
