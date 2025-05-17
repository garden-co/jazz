import { Button } from "@/components/ui/button";
import {
  SiApple,
  SiDiscord,
  SiDropbox,
  SiFacebook,
  SiGithub,
  SiGitlab,
  SiGoogle,
  SiKick,
  SiReddit,
  SiRoblox,
  SiSpotify,
  SiTiktok,
  SiTwitch,
  SiVk,
  SiX,
  SiZoom,
} from "@icons-pack/react-simple-icons";
import { type SSOProviderType, useAuth } from "jazz-react-auth-betterauth";
import { useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { toast } from "sonner";

interface SocialProvider {
  name: string;
  icon?: ReactNode;
}

const socialProviderMap: Record<SSOProviderType, SocialProvider> = {
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
};

interface Props {
  provider: SSOProviderType;
}

export function SSOButton({ provider }: Props) {
  const auth = useAuth();
  const router = useRouter();

  return (
    <Button
      type="button"
      variant="outline"
      onClick={() => {
        auth.authClient.signIn.social(
          {
            provider,
          },
          {
            onSuccess: () => {
              router.push("/");
            },
            onError: (error) => {
              toast.error("Error", {
                description: error.error.message,
              });
            },
          },
        );
      }}
    >
      {socialProviderMap[provider].icon}
      Continue with {socialProviderMap[provider].name}
    </Button>
  );
}
