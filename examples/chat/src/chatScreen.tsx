import { Account } from "jazz-tools";
import { createImage } from "jazz-tools/media";
import { useSuspenseAccount, useSuspenseCoState } from "jazz-tools/react";
import { useRef, useState } from "react";
import { Chat, Message, MessageSnapshot } from "./schema.ts";
import {
  BubbleActions,
  BubbleBody,
  BubbleContainer,
  BubbleImage,
  BubbleInfo,
  BubbleText,
  ChatBody,
  EmptyChatMessage,
  ImageInput,
  InputBar,
  InputBarBanner,
  ReplyPreviewBubble,
  TextInput,
} from "./ui.tsx";
import { useCoStates } from "jazz-tools/react-core";

const INITIAL_MESSAGES_TO_SHOW = 30;

export function ChatScreen(props: { chatID: string }) {
  const chat = useSuspenseCoState(Chat, props.chatID);
  const me = useSuspenseAccount();
  const [showNLastMessages, setShowNLastMessages] = useState(
    INITIAL_MESSAGES_TO_SHOW,
  );
  const [replyingTo, setReplyingTo] = useState<Message | null>(null);
  const [editingMessage, setEditingMessage] = useState<Message | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const messageIds = Array.from(chat.$jazz.refs)
    // We call slice before reverse to avoid mutating the original array
    .slice(-showNLastMessages)
    // Reverse plus flex-col-reverse on ChatBody gives us scroll-to-bottom behavior
    .reverse()
    .map((msgRef) => msgRef.id);

  const messages = useCoStates(Message, messageIds);

  const sendImage = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.currentTarget.files?.[0];

    if (!file) return;

    if (file.size > 5000000) {
      alert("Please upload an image less than 5MB.");
      return;
    }

    createImage(file, {
      owner: chat.$jazz.owner,
      progressive: true,
      placeholder: "blur",
    }).then((image) => {
      chat.$jazz.push({
        text: file.name,
        image: image,
      });
    });
  };

  const handleReply = (msg: Message) => {
    setEditingMessage(null);
    setReplyingTo(msg);
    if (inputRef.current) {
      inputRef.current.value = "";
      inputRef.current.focus();
    }
  };

  const handleEdit = (msg: Message) => {
    setReplyingTo(null);
    setEditingMessage(msg);
    if (inputRef.current) {
      inputRef.current.value = msg.text.toString();
      inputRef.current.focus();
    }
  };

  const handleCancelReply = () => {
    setReplyingTo(null);
    inputRef.current?.focus();
  };

  const handleCancelEdit = () => {
    setEditingMessage(null);
    if (inputRef.current) {
      inputRef.current.value = "";
      inputRef.current.focus();
    }
  };

  const handleSubmit = async (text: string) => {
    if (editingMessage) {
      editingMessage.text.$jazz.applyDiff(text);
      setEditingMessage(null);
    } else if (replyingTo) {
      const snapshot = await MessageSnapshot.create(replyingTo, {
        owner: chat.$jazz.owner,
      });
      chat.$jazz.push({ text, replyOf: snapshot });
      setReplyingTo(null);
    } else {
      chat.$jazz.push({ text });
    }
  };

  return (
    <>
      <ChatBody>
        {messages.length > 0 ? (
          messages.map((msg) =>
            msg.$isLoaded ? (
              <ChatBubble
                me={me}
                msg={msg}
                key={msg.$jazz.id}
                onReply={handleReply}
                onEdit={handleEdit}
              />
            ) : null,
          )
        ) : (
          <EmptyChatMessage />
        )}
        {chat.length > showNLastMessages && (
          <button
            className="px-4 py-1 block mx-auto my-2 border rounded"
            onClick={() => setShowNLastMessages(showNLastMessages + 10)}
          >
            Show more
          </button>
        )}
      </ChatBody>

      <InputBar>
        {replyingTo && (
          <InputBarBanner
            label="Replying to"
            text={replyingTo.text.toString()}
            onCancel={handleCancelReply}
          />
        )}
        {editingMessage && (
          <InputBarBanner label="Editing message" onCancel={handleCancelEdit} />
        )}
        <div className="flex gap-1">
          {!editingMessage && <ImageInput onImageChange={sendImage} />}
          <TextInput
            inputRef={inputRef}
            onSubmit={handleSubmit}
            onCancel={
              editingMessage
                ? handleCancelEdit
                : replyingTo
                  ? handleCancelReply
                  : undefined
            }
            placeholder={
              editingMessage
                ? "Edit message..."
                : replyingTo
                  ? "Reply..."
                  : "Message"
            }
          />
        </div>
      </InputBar>
    </>
  );
}

function ChatBubble({
  me,
  msg,
  onReply,
  onEdit,
}: {
  me: Account;
  msg: Message;
  onReply: (msg: Message) => void;
  onEdit: (msg: Message) => void;
}) {
  const fromMe = msg.$jazz.createdBy === me.$jazz.id;
  const isEdited =
    msg.text.$jazz.raw.core.latestTxMadeAt >
    msg.text.$jazz.raw.core.earliestTxMadeAt;

  return (
    <BubbleContainer fromMe={fromMe} messageId={msg.$jazz.id}>
      <BubbleInfo by={msg.$jazz.createdBy} madeAt={msg.$jazz.createdAt} />
      <div className="group relative max-w-[calc(100%-5rem)]">
        <BubbleBody fromMe={fromMe}>
          {msg.replyOf ? (
            <ReplyContext replyOf={msg.replyOf} fromMe={fromMe} />
          ) : null}
          {msg.image ? <BubbleImage image={msg.image} /> : null}
          <BubbleText text={msg.text} />
          {isEdited ? (
            <div className="text-right text-xs opacity-70 mr-2">Edited</div>
          ) : null}
        </BubbleBody>
        <BubbleActions
          fromMe={fromMe}
          onReply={() => onReply(msg)}
          onEdit={fromMe ? () => onEdit(msg) : undefined}
        />
      </div>
    </BubbleContainer>
  );
}

function ReplyContext({
  replyOf,
  fromMe,
}: {
  replyOf: MessageSnapshot;
  fromMe: boolean;
}) {
  const scrollToOriginal = () => {
    const el = document.querySelector(
      `[data-message-id="${replyOf.ref.$jazz.id}"]`,
    );
    if (!el) return;

    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.animate(
      [
        { backgroundColor: "rgba(59,130,246,0.3)" },
        { backgroundColor: "transparent" },
      ],
      { duration: 2500, easing: "ease-out" },
    );
  };

  return (
    <ReplyPreviewBubble
      text={replyOf.ref.text.toString()}
      fromMe={fromMe}
      onClick={scrollToOriginal}
    />
  );
}
