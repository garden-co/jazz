interface ChatMetadataProps {
  date: Date;
  senderName?: string;
}

export const ChatMetadata = ({ date, senderName }: ChatMetadataProps) => {
  const dateToString = date.toLocaleTimeString();
  return (
    <div className="text-xs gap-1 flex mb-1 text-muted-foreground">
      {senderName && <span>{senderName}</span>}
      <span>&bull;</span>
      <span>{dateToString}</span>
    </div>
  );
};
