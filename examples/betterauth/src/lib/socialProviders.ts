const apple =
  process.env.APPLE_CLIENT_ID &&
  process.env.APPLE_CLIENT_SECRET &&
  process.env.APPLE_APP_BUNDLE_IDENTIFIER
    ? {
        apple: {
          clientId: process.env.APPLE_CLIENT_ID as string,
          clientSecret: process.env.APPLE_CLIENT_SECRET as string,
          // For native iOS
          appBundleIdentifier: process.env
            .APPLE_APP_BUNDLE_IDENTIFIER as string,
        },
      }
    : undefined;
const discord =
  process.env.DISCORD_CLIENT_ID && process.env.DISCORD_CLIENT_SECRET
    ? {
        discord: {
          clientId: process.env.DISCORD_CLIENT_ID as string,
          clientSecret: process.env.DISCORD_CLIENT_SECRET as string,
        },
      }
    : undefined;
const facebook =
  process.env.FACEBOOK_CLIENT_ID && process.env.FACEBOOK_CLIENT_SECRET
    ? {
        facebook: {
          clientId: process.env.FACEBOOK_CLIENT_ID as string,
          clientSecret: process.env.FACEBOOK_CLIENT_SECRET as string,
        },
      }
    : undefined;
const github =
  process.env.GITHUB_CLIENT_ID && process.env.GITHUB_CLIENT_SECRET
    ? {
        github: {
          clientId: process.env.GITHUB_CLIENT_ID as string,
          clientSecret: process.env.GITHUB_CLIENT_SECRET as string,
        },
      }
    : undefined;
const google =
  process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET
    ? {
        google: {
          clientId: process.env.GOOGLE_CLIENT_ID as string,
          clientSecret: process.env.GOOGLE_CLIENT_SECRET as string,
        },
      }
    : undefined;
const kick =
  process.env.KICK_CLIENT_ID && process.env.KICK_CLIENT_SECRET
    ? {
        kick: {
          clientId: process.env.KICK_CLIENT_ID as string,
          clientSecret: process.env.KICK_CLIENT_SECRET as string,
        },
      }
    : undefined;
const microsoft =
  process.env.MICROSOFT_CLIENT_ID && process.env.MICROSOFT_CLIENT_SECRET
    ? {
        microsoft: {
          clientId: process.env.MICROSOFT_CLIENT_ID as string,
          clientSecret: process.env.MICROSOFT_CLIENT_SECRET as string,
          tenantId: "common",
          requireSelectAccount: true,
        },
      }
    : undefined;
const tiktok =
  process.env.TIKTOK_CLIENT_ID &&
  process.env.TIKTOK_CLIENT_SECRET &&
  process.env.TIKTOK_CLIENT_KEY
    ? {
        tiktok: {
          clientId: process.env.TIKTOK_CLIENT_ID,
          clientSecret: process.env.TIKTOK_CLIENT_SECRET,
          clientKey: process.env.TIKTOK_CLIENT_KEY,
        },
      }
    : undefined;
const twitch =
  process.env.TWITCH_CLIENT_ID && process.env.TWITCH_CLIENT_SECRET
    ? {
        twitch: {
          clientId: process.env.TWITCH_CLIENT_ID as string,
          clientSecret: process.env.TWITCH_CLIENT_SECRET as string,
        },
      }
    : undefined;
const twitter =
  process.env.TWITTER_CLIENT_ID && process.env.TWITTER_CLIENT_SECRET
    ? {
        twitter: {
          clientId: process.env.TWITTER_CLIENT_ID,
          clientSecret: process.env.TWITTER_CLIENT_SECRET,
        },
      }
    : undefined;
const dropbox =
  process.env.DROPBOX_CLIENT_ID && process.env.DROPBOX_CLIENT_SECRET
    ? {
        dropbox: {
          clientId: process.env.DROPBOX_CLIENT_ID as string,
          clientSecret: process.env.DROPBOX_CLIENT_SECRET as string,
        },
      }
    : undefined;
const linkedin =
  process.env.LINKEDIN_CLIENT_ID && process.env.LINKEDIN_CLIENT_SECRET
    ? {
        linkedin: {
          clientId: process.env.LINKEDIN_CLIENT_ID as string,
          clientSecret: process.env.LINKEDIN_CLIENT_SECRET as string,
        },
      }
    : undefined;
const gitlab =
  process.env.GITLAB_CLIENT_ID &&
  process.env.GITLAB_CLIENT_SECRET &&
  process.env.GITLAB_ISSUER
    ? {
        gitlab: {
          clientId: process.env.GITLAB_CLIENT_ID as string,
          clientSecret: process.env.GITLAB_CLIENT_SECRET as string,
          issuer: process.env.GITLAB_ISSUER as string,
        },
      }
    : undefined;
const reddit =
  process.env.REDDIT_CLIENT_ID && process.env.REDDIT_CLIENT_SECRET
    ? {
        reddit: {
          clientId: process.env.REDDIT_CLIENT_ID as string,
          clientSecret: process.env.REDDIT_CLIENT_SECRET as string,
        },
      }
    : undefined;
const roblox =
  process.env.ROBLOX_CLIENT_ID && process.env.ROBLOX_CLIENT_SECRET
    ? {
        roblox: {
          clientId: process.env.ROBLOX_CLIENT_ID,
          clientSecret: process.env.ROBLOX_CLIENT_SECRET,
        },
      }
    : undefined;
const spotify =
  process.env.SPOTIFY_CLIENT_ID && process.env.SPOTIFY_CLIENT_SECRET
    ? {
        spotify: {
          clientId: process.env.SPOTIFY_CLIENT_ID as string,
          clientSecret: process.env.SPOTIFY_CLIENT_SECRET as string,
        },
      }
    : undefined;
const vk =
  process.env.VK_CLIENT_ID && process.env.VK_CLIENT_SECRET
    ? {
        vk: {
          clientId: process.env.VK_CLIENT_ID as string,
          clientSecret: process.env.VK_CLIENT_SECRET as string,
        },
      }
    : undefined;

export const socialProviders = {
  ...(apple && { ...apple }),
  ...(discord && { ...discord }),
  ...(facebook && { ...facebook }),
  ...(github && { ...github }),
  ...(google && { ...google }),
  ...(kick && { ...kick }),
  ...(microsoft && { ...microsoft }),
  ...(tiktok && { ...tiktok }),
  ...(twitch && { ...twitch }),
  ...(twitter && { ...twitter }),
  ...(dropbox && { ...dropbox }),
  ...(linkedin && { ...linkedin }),
  ...(gitlab && { ...gitlab }),
  ...(reddit && { ...reddit }),
  ...(roblox && { ...roblox }),
  ...(spotify && { ...spotify }),
  ...(vk && { ...vk }),
};
