import * as Clipboard from "expo-clipboard";
import { Account, CoMapEdit, Group } from "jazz-tools";
import { useState } from "react";
import React, {
  Button,
  FlatList,
  KeyboardAvoidingView,
  SafeAreaView,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  Alert,
  StyleSheet,
} from "react-native";

import { useAccount, useCoState, useLogOut } from "jazz-tools/expo";
import { Chat, Message } from "./schema";

export default function ChatScreen() {
  const me = useAccount(Account, { resolve: { profile: true } });
  const logOut = useLogOut();
  const [chatId, setChatId] = useState<string>();
  const [chatIdInput, setChatIdInput] = useState<string>();
  const loadedChat = useCoState(Chat, chatId, { resolve: { $each: true } });
  const [message, setMessage] = useState("");

  function handleLogOut() {
    setChatId(undefined);
    logOut();
  }

  const createChat = () => {
    const group = Group.create();
    group.addMember("everyone", "writer");
    const chat = Chat.create([], group);
    setChatId(chat.$jazz.id);
  };

  const joinChat = () => {
    if (chatIdInput) {
      if (chatIdInput.startsWith("https://chat.jazz.tools/#/chat/")) {
        setChatId(chatIdInput.split("/").pop());
      } else {
        setChatId(chatIdInput);
      }
    } else {
      Alert.alert("Error", "Chat ID cannot be empty.");
    }
  };

  const sendMessage = () => {
    if (!loadedChat.$isLoaded) return;
    if (message.trim()) {
      loadedChat.$jazz.push(
        Message.create({ text: message }, { owner: loadedChat?.$jazz.owner }),
      );
      setMessage("");
    }
  };

  const renderMessageItem = ({ item }: { item: Message }) => {
    const isMe = item.$jazz.getEdits()?.text?.by?.isMe;
    return (
      <View
        style={[
          styles.messageContainer,
          isMe ? styles.myMessage : styles.otherMessage,
        ]}
      >
        {!isMe ? (
          <Text
            style={[
              styles.messageSender,
              { textAlign: isMe ? "right" : "left" },
            ]}
          >
            {getEditorName(item?.$jazz.getEdits()?.text)}
          </Text>
        ) : null}
        <View style={styles.messageContent}>
          <Text style={styles.messageText}>{item.text}</Text>
          <Text style={[styles.messageTime, { marginTop: !isMe ? 8 : 4 }]}>
            {item?.$jazz
              .getEdits()
              ?.text?.madeAt?.getHours()
              .toString()
              .padStart(2, "0")}
            :
            {item?.$jazz
              .getEdits()
              ?.text?.madeAt?.getMinutes()
              .toString()
              .padStart(2, "0")}
          </Text>
        </View>
      </View>
    );
  };

  return (
    <View style={styles.container}>
      {!loadedChat.$isLoaded ? (
        <View style={styles.welcomeContainer}>
          <Text style={styles.usernameTitle}>Username</Text>
          <TextInput
            style={styles.usernameInput}
            value={me.$isLoaded ? me.profile.name : ""}
            onChangeText={(value) => {
              if (me.$isLoaded) {
                me.profile.$jazz.set("name", value);
              }
            }}
            textAlignVertical="center"
            onSubmitEditing={sendMessage}
            testID="username-input"
          />
          <TouchableOpacity onPress={createChat} style={styles.newChatButton}>
            <Text style={styles.newChatButtonText}>Start new chat</Text>
          </TouchableOpacity>
          <Text style={styles.joinChatTitle}>Join existing chat</Text>
          <TextInput
            style={styles.chatIdInput}
            placeholder="Chat ID"
            value={chatIdInput ?? ""}
            onChangeText={(value) => {
              setChatIdInput(value);
            }}
            textAlignVertical="center"
            onSubmitEditing={() => {
              if (chatIdInput) {
                setChatId(chatIdInput);
              }
            }}
            testID="chat-id-input"
          />
          <TouchableOpacity
            testID="join-chat-button"
            onPress={joinChat}
            style={styles.joinChatButton}
          >
            <Text style={styles.newChatButtonText}>Join chat</Text>
          </TouchableOpacity>
        </View>
      ) : (
        <>
          <View style={styles.chatHeader}>
            <Button
              onPress={() => {
                if (loadedChat?.$jazz.id) {
                  Clipboard.setStringAsync(
                    `https://chat.jazz.tools/#/chat/${loadedChat.$jazz.id}`,
                  );
                  Alert.alert(
                    "Copied to clipboard",
                    `Chat ID: ${loadedChat.$jazz.id}`,
                  );
                }
              }}
              title="Share"
            />
            <Text style={{ fontWeight: "bold", fontSize: 18 }}>Jazz chat</Text>
            <Button onPress={handleLogOut} title="Logout" />
          </View>
          <FlatList
            contentContainerStyle={{
              flexGrow: 1,
              gap: 6,
              padding: 8,
              justifyContent: "flex-end",
            }}
            style={styles.messageList}
            data={loadedChat}
            keyExtractor={(item) => item.$jazz.id}
            renderItem={renderMessageItem}
          />

          <KeyboardAvoidingView
            keyboardVerticalOffset={110}
            behavior="padding"
            style={styles.inputContainer}
          >
            <SafeAreaView style={styles.inputRow}>
              <TextInput
                style={styles.messageInput}
                value={message}
                onChangeText={setMessage}
                placeholder="Type a message..."
                textAlignVertical="center"
                onSubmitEditing={sendMessage}
                testID="message-input"
              />
              <TouchableOpacity
                onPress={sendMessage}
                style={styles.sendButton}
                testID="send-button"
              >
                <Text>↑</Text>
              </TouchableOpacity>
            </SafeAreaView>
          </KeyboardAvoidingView>
        </>
      )}
    </View>
  );
}

function getEditorName(edit?: CoMapEdit<unknown>): string | undefined {
  if (!edit?.by?.profile || !edit.by.profile.$isLoaded) {
    return;
  }
  return edit.by.profile.name;
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    flexDirection: "column",
    height: "100%",
  },
  chatHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    padding: 8,
    paddingTop: 48,
    backgroundColor: "#f0f0f0",
  },
  welcomeContainer: {
    flex: 1,
    flexDirection: "column",
    height: "100%",
    alignItems: "center",
    justifyContent: "center",
  },
  usernameTitle: {
    fontSize: 16,
    fontWeight: "bold",
    marginBottom: 24,
  },
  usernameInput: {
    borderRadius: 4,
    height: 48,
    padding: 8,
    marginBottom: 48,
    width: 160,
    borderWidth: 1,
    borderColor: "#e5e5e5",
  },
  newChatButton: {
    backgroundColor: "#3b82f6",
    padding: 16,
    borderRadius: 6,
  },
  newChatButtonText: {
    color: "white",
    fontWeight: "600",
  },
  joinChatTitle: {
    fontSize: 16,
    fontWeight: "bold",
    marginTop: 24,
  },
  chatIdInput: {
    borderRadius: 4,
    height: 48,
    padding: 8,
    margin: 8,
    marginTop: 16,
    width: 320,
    borderWidth: 1,
    borderColor: "#e5e5e5",
  },
  joinChatButton: {
    backgroundColor: "#22c55e",
    padding: 16,
    borderRadius: 6,
  },
  messageList: {
    display: "flex",
  },
  messageContainer: {
    borderRadius: 8,
    padding: 4,
    paddingHorizontal: 6,
    maxWidth: "80%",
  },
  myMessage: {
    backgroundColor: "#e5e5e5",
    alignSelf: "flex-end",
  },
  otherMessage: {
    backgroundColor: "#d4d4d4",
    alignSelf: "flex-start",
  },
  messageSender: {
    fontSize: 12,
    color: "#6b7280",
  },
  messageContent: {
    flexDirection: "row",
    alignItems: "flex-end",
    justifyContent: "space-between",
  },
  messageText: {
    color: "black",
    fontSize: 16,
    maxWidth: "85%",
  },
  messageTime: {
    fontSize: 10,
    color: "#6b7280",
    marginLeft: 8,
  },
  inputContainer: {
    padding: 12,
    backgroundColor: "white",
    borderTopWidth: 1,
    borderTopColor: "#d4d4d4",
  },
  inputRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  messageInput: {
    borderRadius: 9999,
    height: 32,
    paddingVertical: 0,
    paddingHorizontal: 8,
    borderWidth: 1,
    borderColor: "#e5e5e5",
    flex: 1,
  },
  sendButton: {
    backgroundColor: "#d4d4d4",
    borderRadius: 9999,
    height: 32,
    width: 32,
    alignItems: "center",
    justifyContent: "center",
  },
});
