export interface BandChatConfig {
  nodeEnv: string | undefined;
  unsetPublic: string[];
  origin: string;
  appId: string | undefined;
  serverUrl: string | undefined;
  backendSecret: string | undefined;
  betterAuthSecret: string | undefined;
}
export declare const LOCAL_ORIGIN: string;
export declare function readConfig(env?: Record<string, string | undefined>): BandChatConfig;
export declare function usesLocalDefaults(config?: BandChatConfig): boolean;
export declare function assertConfiguration(
  config?: BandChatConfig,
): BandChatConfig & { backendSecret: string; betterAuthSecret: string };
export declare function jazzServer(config?: BandChatConfig): { appId: string; serverUrl: string };
