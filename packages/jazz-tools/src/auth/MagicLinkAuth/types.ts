export interface MagicLinkAuthConsumerOptions {
  handlerTimeout?: number;
  onLoggedIn?: () => void;
}

export interface MagicLinkAuthProviderOptions {
  expireInMs?: number;
}
