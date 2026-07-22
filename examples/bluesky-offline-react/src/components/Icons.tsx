import {
  ArrowLeftIcon,
  ChatBubbleIcon,
  CheckIcon,
  ChevronRightIcon,
  DotFilledIcon,
  ExitIcon,
  ExternalLinkIcon,
  HeartFilledIcon,
  HeartIcon,
  LoopIcon,
} from "@radix-ui/react-icons";

export function BackIcon() {
  return <ArrowLeftIcon aria-hidden="true" />;
}

export function DisclosureIcon() {
  return <ChevronRightIcon aria-hidden="true" />;
}

export function LikeIcon({ active }: { active: boolean }) {
  return active ? <HeartFilledIcon aria-hidden="true" /> : <HeartIcon aria-hidden="true" />;
}

export function ReplyIcon() {
  return <ChatBubbleIcon aria-hidden="true" />;
}

export function RepostIcon() {
  return <LoopIcon aria-hidden="true" />;
}

export function StatusIcon() {
  return <DotFilledIcon aria-hidden="true" />;
}

export function SignOutIcon() {
  return <ExitIcon aria-hidden="true" />;
}

export function SuccessIcon() {
  return <CheckIcon aria-hidden="true" />;
}

export function ThreadLinkIcon() {
  return <ExternalLinkIcon aria-hidden="true" />;
}
