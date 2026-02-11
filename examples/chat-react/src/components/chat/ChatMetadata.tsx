import { useCoState } from "jazz-tools/react";
import { ChatAccountWithProfile } from "@/schema";

interface ChatMetadataProps {
  date: number;
  sender?: string;
}

export const ChatMetadata = ({ date, sender }: ChatMetadataProps) => {
  const dateToString = new Date(date).toLocaleTimeString();
  const senderAccount = useCoState(ChatAccountWithProfile, sender);
  return (
    <div className="text-xs gap-1 flex mb-1 text-muted-foreground">
      {senderAccount.$isLoaded && <span>{senderAccount.profile.name}</span>}
      <span>&bull;</span>
      <span>{dateToString}</span>
    </div>
  );
};
