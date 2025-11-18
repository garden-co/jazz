import Clipboard from "@react-native-clipboard/clipboard";
import { launchImageLibrary } from "react-native-image-picker";
import {
  co,
  CoPlainText,
  getLoadedOrUndefined,
  Group,
  ID,
  ImageDefinition,
  LastAndAllCoMapEdits,
  Loaded,
} from "jazz-tools";
import {
  useAccount,
  useCoState,
  useLogOut,
  Image,
} from "jazz-tools/react-native";
import { useEffect, useRef, useState } from "react";
import {
  Animated,
  Button,
  Easing,
  FlatList,
  KeyboardAvoidingView,
  SafeAreaView,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from "react-native";
import { Chat, Message } from "./schema";
import { createImage } from "jazz-tools/media";

export function ChatScreen({ navigation }: { navigation: any }) {
  const me = useAccount();
  const logOut = useLogOut();
  const [chatId, setChatId] = useState<string>();
  const [chatIdInput, setChatIdInput] = useState<string>();
  const loadedChat = useCoState(Chat, chatId, {
    resolve: { $each: { text: true } },
  });
  const [message, setMessage] = useState("");
  const profile = getLoadedOrUndefined(me)?.profile;
  const [imageUploading, setImageUploading] = useState(false);
  const spinAnim = useRef(new Animated.Value(0)).current;

  function handleLogOut() {
    setChatId(undefined);
    logOut();
  }

  useEffect(() => {
    navigation.setOptions({
      headerRight: () => <Button onPress={handleLogOut} title="Logout" />,
      headerLeft: () =>
        loadedChat ? (
          <Button
            onPress={() => {
              if (loadedChat?.$jazz.id) {
                Clipboard.setString(loadedChat.$jazz.id);
                console.log(
                  "Copied to clipboard",
                  `Chat ID: ${loadedChat.$jazz.id}`,
                );
              }
            }}
            title="Share"
          />
        ) : null,
    });
  }, [navigation, loadedChat]);

  useEffect(() => {
    let loop: Animated.CompositeAnimation | null = null;

    if (imageUploading) {
      const flip = Animated.sequence([
        Animated.timing(spinAnim, {
          toValue: 1,
          duration: 400, // quick flip
          easing: Easing.inOut(Easing.ease),
          useNativeDriver: true,
        }),
        Animated.delay(700), // pause before next flip
      ]);

      loop = Animated.loop(flip);
      loop.start();
    } else {
      spinAnim.stopAnimation();
      spinAnim.setValue(0);
    }

    return () => {
      loop?.stop();
    };
  }, [imageUploading]);

  const spin = spinAnim.interpolate({
    inputRange: [0, 1],
    outputRange: ["0deg", "180deg"],
  });

  const createChat = () => {
    if (!me.$isLoaded) return;
    const group = Group.create({ owner: me });
    group.addMember("everyone", "writer");
    const chat = Chat.create([], { owner: group });
    setChatId(chat.$jazz.id);
  };

  const joinChat = () => {
    if (chatIdInput) {
      setChatId(chatIdInput as ID<typeof Chat>);
    } else {
      console.warn("Error: Chat ID cannot be empty.");
    }
  };

  const sendMessage = () => {
    if (!loadedChat.$isLoaded) return;
    if (message.trim()) {
      loadedChat.$jazz.push(
        Message.create(
          { text: co.plainText().create(message, loadedChat.$jazz.owner) },
          loadedChat.$jazz.owner,
        ),
      );
      setMessage("");
    }
  };

  const sendPhoto = async () => {
    setImageUploading(true);
    try {
      if (!loadedChat.$isLoaded || !me.$isLoaded)
        throw new Error("Chat or user not loaded");

      const result = await launchImageLibrary({
        mediaType: "photo",
        quality: 0.8,
      });

      if (!result.didCancel && result.assets?.[0].uri) {
        const image = await createImage(result.assets[0].uri, {
          owner: loadedChat.$jazz.owner,
          placeholder: "blur",
          maxSize: 1024,
        });

        const thisMessage = Message.create(
          {
            text: co
              .plainText()
              .create(message ? message.trim() : "", loadedChat.$jazz.owner),
            image,
          },
          loadedChat.$jazz.owner,
        );

        loadedChat.$jazz.push(thisMessage);

        setMessage("");
      }
    } catch (error) {
      console.error(error);
    } finally {
      setImageUploading(false);
    }
  };

  const renderMessageItem = ({
    item,
  }: {
    item: Loaded<typeof Message, { text: true }>;
  }) => {
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
              styles.senderName,
              isMe ? styles.textRight : styles.textLeft,
            ]}
          >
            {getEditorName(item?.$jazz.getEdits()?.text)}
          </Text>
        ) : null}
        <View style={styles.messageContent}>
          {item.image && (
            <Image
              imageId={item.image?.$jazz.id}
              width={200}
              height="original"
            />
          )}
          <Text style={styles.messageText}>{item.text}</Text>
          <Text
            style={[
              styles.timestamp,
              !isMe ? styles.timestampOther : styles.timestampMy,
            ]}
          >
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
    <SafeAreaView style={styles.container}>
      {!loadedChat.$isLoaded ? (
        <View style={styles.welcomeContainer}>
          <Text style={styles.usernameLabel}>Username</Text>
          <TextInput
            style={styles.usernameInput}
            value={getLoadedOrUndefined(profile)?.name ?? ""}
            onChangeText={(value) => {
              if (profile?.$isLoaded) {
                profile.$jazz.set("name", value);
              }
            }}
            textAlignVertical="center"
            onSubmitEditing={sendMessage}
            testID="username-input"
          />
          <TouchableOpacity onPress={createChat} style={styles.newChatButton}>
            <Text style={styles.buttonText}>Start new chat</Text>
          </TouchableOpacity>
          <Text style={styles.usernameLabel}>Join existing chat</Text>
          <TextInput
            style={styles.chatIdInput}
            placeholder="Chat ID"
            value={chatIdInput ?? ""}
            onChangeText={setChatIdInput}
            textAlignVertical="center"
            onSubmitEditing={joinChat}
            testID="chat-id-input"
          />
          <TouchableOpacity onPress={joinChat} style={styles.joinChatButton}>
            <Text style={styles.buttonText}>Join chat</Text>
          </TouchableOpacity>
        </View>
      ) : (
        <>
          <FlatList
            contentContainerStyle={{
              flexGrow: 1,
              flex: 1,
              gap: 6,
              padding: 8,
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
            <View style={styles.inputWrapper}>
              <TextInput
                style={styles.messageInput}
                value={imageUploading ? "Uploading..." : message}
                onChangeText={imageUploading ? undefined : (v) => setMessage(v)}
                placeholder={
                  imageUploading ? "Uploading..." : "Type a message..."
                }
                textAlignVertical="center"
                onSubmitEditing={imageUploading ? undefined : sendMessage}
                testID="message-input"
              />
              <TouchableOpacity
                onPress={imageUploading ? undefined : sendPhoto}
                style={styles.sendButton}
                testID="send-photo-button"
              >
                {imageUploading ? (
                  <Animated.Text style={{ transform: [{ rotate: spin }] }}>
                    ⌛
                  </Animated.Text>
                ) : (
                  <Text>📸</Text>
                )}
              </TouchableOpacity>
              <TouchableOpacity
                onPress={imageUploading ? undefined : sendMessage}
                style={styles.sendButton}
                testID="send-button"
              >
                <Text>↑</Text>
              </TouchableOpacity>
            </View>
          </KeyboardAvoidingView>
        </>
      )}
    </SafeAreaView>
  );
}

function getEditorName(
  edit?: LastAndAllCoMapEdits<CoPlainText>,
): string | undefined {
  if (!edit?.by?.profile || !edit.by.profile.$isLoaded) {
    return;
  }
  return edit.by.profile.name;
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  welcomeContainer: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 16,
  },
  usernameLabel: {
    fontSize: 16,
    fontWeight: "bold",
    marginBottom: 16,
  },
  usernameInput: {
    width: 160,
    height: 48,
    borderWidth: 1,
    borderColor: "#ddd",
    borderRadius: 8,
    paddingHorizontal: 8,
    marginBottom: 24,
  },
  chatIdInput: {
    width: 320,
    height: 48,
    borderWidth: 1,
    borderColor: "#ddd",
    borderRadius: 8,
    paddingHorizontal: 8,
    marginBottom: 24,
    marginTop: 8,
  },
  newChatButton: {
    backgroundColor: "#3B82F6",
    padding: 16,
    borderRadius: 8,
    marginBottom: 24,
  },
  joinChatButton: {
    backgroundColor: "#10B981",
    padding: 16,
    borderRadius: 8,
  },
  buttonText: {
    color: "white",
    fontSize: 16,
    fontWeight: "bold",
    textAlign: "center",
  },
  messageList: {
    flex: 1,
  },
  inputContainer: {
    padding: 12,
  },
  inputWrapper: {
    flexDirection: "row",
    alignItems: "center",
    borderWidth: 1,
    borderColor: "#ddd",
    borderRadius: 8,
    paddingHorizontal: 12,
  },
  messageInput: {
    flex: 1,
    height: 42,
    paddingHorizontal: 8,
  },
  sendButton: {
    paddingHorizontal: 16,
  },
  messageContainer: {
    flexDirection: "column",
    alignItems: "flex-start",
    marginBottom: 8,
  },
  myMessage: {
    alignSelf: "flex-end",
  },
  otherMessage: {
    alignSelf: "flex-start",
  },
  senderName: {
    fontSize: 12,
    color: "#666",
    marginBottom: 4,
  },
  textRight: {
    textAlign: "right",
  },
  textLeft: {
    textAlign: "left",
  },
  messageContent: {
    backgroundColor: "#f0f0f0",
    borderRadius: 8,
    padding: 8,
  },
  messageText: {
    fontSize: 16,
  },
  timestamp: {
    fontSize: 10,
    color: "#666",
    marginTop: 4,
  },
  timestampOther: {
    textAlign: "left",
    marginTop: 8,
  },
  timestampMy: {
    textAlign: "right",
    marginTop: 4,
  },
  avatar: {
    borderRadius: 16,
    height: 32,
    width: 32,
    alignItems: "center",
    justifyContent: "center",
  },
});
