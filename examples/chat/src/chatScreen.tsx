import { Account } from "jazz-tools";
import { createImage } from "jazz-tools/media";
import { useSuspenseAccount, useSuspenseCoState } from "jazz-tools/react";
import { useState } from "react";
import { Chat, Message } from "./schema.ts";
import {
  BubbleBody,
  BubbleContainer,
  BubbleImage,
  BubbleInfo,
  BubbleText,
  ChatBody,
  EmptyChatMessage,
  ImageInput,
  InputBar,
  TextInput,
} from "./ui.tsx";
import { useMultiCoState } from "jazz-tools/react-core";

const INITIAL_MESSAGES_TO_SHOW = 30;

const ChatWithMessages = Chat.resolved({
  $each: true,
});

export function ChatScreen(props: { chatID: string }) {
  const chat = useSuspenseCoState(ChatWithMessages, props.chatID);
  const me = useSuspenseAccount();
  const [showNLastMessages, setShowNLastMessages] = useState(
    INITIAL_MESSAGES_TO_SHOW,
  );

  const messages = useMultiCoState(
    chat
      // We call slice before reverse to avoid mutating the original array
      .slice(-showNLastMessages)
      // Reverse plus flex-col-reverse on ChatBody gives us scroll-to-bottom behavior
      .reverse()
      .map((msg) => ({
        schema: Message,
        id: msg.$jazz.id,
      })),
  );

  // The initial messages should be loaded all at once, so we can avoid flickering
  if (
    messages.slice(0, INITIAL_MESSAGES_TO_SHOW).some((msg) => !msg.$isLoaded)
  ) {
    return null;
  }

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

  return (
    <>
      <ChatBody>
        {messages.length > 0 ? (
          messages.map((msg) =>
            msg.$isLoaded ? (
              <ChatBubble me={me} msg={msg} key={msg.$jazz.id} />
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
        <ImageInput onImageChange={sendImage} />

        <TextInput
          onSubmit={(text) => {
            chat.$jazz.push({ text });
          }}
        />
      </InputBar>
    </>
  );
}

function ChatBubble({ me, msg }: { me: Account; msg: Message }) {
  const fromMe = msg.$jazz.createdBy === me.$jazz.id;

  return (
    <BubbleContainer fromMe={fromMe}>
      <BubbleInfo by={msg.$jazz.createdBy} madeAt={msg.$jazz.createdAt} />
      <BubbleBody fromMe={fromMe}>
        {msg.image ? <BubbleImage image={msg.image} /> : null}
        <BubbleText text={msg.text} />
      </BubbleBody>
    </BubbleContainer>
  );
}
