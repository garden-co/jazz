export interface BandChatConfig {
  nodeEnv: string | undefined;
  unsetPublic: string[];
  origin: string;
  appId: string;
  betterAuthSecret: string | undefined;
  remote:
    | { url: string; adminSecret: string | undefined; backendSecret: string | undefined }
    | undefined;
  dataPath: string;
}
export declare const LOCAL_ORIGIN: string;
export declare const LOCAL_APP_ID: string;
export declare function readConfig(env?: Record<string, string | undefined>): BandChatConfig;
export declare function usesLocalDefaults(config?: BandChatConfig): boolean;
export declare function assertConfiguration(config?: BandChatConfig): BandChatConfig & {
  betterAuthSecret: string;
  remote: { url: string; adminSecret: string; backendSecret: string } | undefined;
};
