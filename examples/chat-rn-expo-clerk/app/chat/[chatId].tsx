import { Chat, Message } from "@/src/schema";
import { useNavigation } from "@react-navigation/native";
import clsx from "clsx";
import * as Clipboard from "expo-clipboard";
import * as ImagePicker from "expo-image-picker";
import { useLocalSearchParams } from "expo-router";
import { useAccount, useCoState } from "jazz-expo";
import { ProgressiveImg } from "jazz-expo";
import { createImage } from "jazz-react-native-media-images";
import { CoPlainText, Group, ID } from "jazz-tools";
import { useEffect, useLayoutEffect, useState } from "react";
import React, {
  SafeAreaView,
  View,
  Text,
  Alert,
  TouchableOpacity,
  FlatList,
  KeyboardAvoidingView,
  TextInput,
  Button,
  Image,
  ActivityIndicator,
} from "react-native";

export default function Conversation() {
  const { chatId } = useLocalSearchParams();
  const { me } = useAccount();
  const [chat, setChat] = useState<Chat>();
  const [message, setMessage] = useState("");
  const loadedChat = useCoState(Chat, chat?.id, { resolve: { $each: true } });
  const navigation = useNavigation();
  const [isUploading, setIsUploading] = useState(false);

  useEffect(() => {
    if (chat) return;
    if (chatId === "new") {
      createChat();
    } else {
      loadChat(chatId as ID<Chat>);
    }
  }, [chat]);

  // Effect to dynamically set header options
  useLayoutEffect(() => {
    navigation.setOptions({
      headerTitle: "Chat",
      headerRight: () =>
        chat ? (
          <Button
            onPress={() => {
              if (chat?.id) {
                Clipboard.setStringAsync(
                  `https://chat.jazz.tools/#/chat/${chat.id}`,
                );
                Alert.alert("Copied to clipboard", `Chat ID: ${chat.id}`);
              }
            }}
            title="Share"
          />
        ) : null,
    });
  }, [navigation, chat]);

  const createChat = () => {
    const group = Group.create({ owner: me });
    group.addMember("everyone", "writer");
    const chat = Chat.create([], { owner: group });
    setChat(chat);
  };

  const loadChat = async (chatId: ID<Chat>) => {
    try {
      const chat = await Chat.load(chatId);
      if (chat) setChat(chat);
    } catch (error) {
      console.log("Error loading chat", error);
      Alert.alert("Error", `Error loading chat: ${error}`);
    }
  };

  const sendMessage = () => {
    if (!chat) return;
    if (message.trim()) {
      chat.push(
        Message.create(
          { text: CoPlainText.create(message, chat._owner) },
          chat._owner,
        ),
      );
      setMessage("");
    }
  };

  const handleImageUpload = async () => {
    try {
      const result = await ImagePicker.launchImageLibraryAsync({
        mediaTypes: ImagePicker.MediaTypeOptions.Images,
        base64: true,
        quality: 0.7,
      });

      if (!result.canceled && result.assets[0].base64 && chat) {
        setIsUploading(true);
        const base64Uri = `data:image/jpeg;base64,${result.assets[0].base64}`;

        const image = await createImage(base64Uri, {
          owner: chat._owner,
          maxSize: 2048,
        });

        chat.push(
          Message.create(
            { text: CoPlainText.create("", chat._owner), image },
            chat._owner,
          ),
        );
      }
    } catch (error) {
      Alert.alert("Error", "Failed to upload image");
    } finally {
      setIsUploading(false);
    }
  };

  const renderMessageItem = ({ item }: { item: Message }) => {
    const isMe = item._edits.text.by?.isMe;
    return (
      <View
        className={clsx(
          `rounded-xl px-3 py-2 max-w-[75%] my-1`,
          isMe ? `bg-blue-500 self-end` : `bg-gray-200 self-start`,
        )}
      >
        {!isMe ? (
          <Text
            className={clsx(
              `text-xs text-gray-500 mb-1`,
              isMe ? "text-right" : "text-left",
            )}
          >
            {item._edits.text.by?.profile?.name}
          </Text>
        ) : null}
        <View
          className={clsx(
            "flex relative items-end justify-between",
            isMe ? "flex-row" : "flex-row",
          )}
        >
          {item.image && (
            <ProgressiveImg image={item.image} maxWidth={1024}>
              {({ src, res, originalSize }) => (
                <Image
                  source={{ uri: src }}
                  className="w-48 h-48 rounded-lg mb-2"
                  resizeMode="cover"
                />
              )}
            </ProgressiveImg>
          )}
          {item.text && (
            <Text
              className={clsx(
                !isMe ? "text-black" : "text-gray-200",
                `text-md max-w-[85%]`,
              )}
            >
              {item.text}
            </Text>
          )}
          <Text
            className={clsx(
              "text-[10px] text-right ml-2",
              !isMe ? "mt-2 text-gray-500" : "mt-1 text-gray-200",
            )}
          >
            {item._edits.text.madeAt?.getHours().toString().padStart(2, "0")}:
            {item._edits.text.madeAt?.getMinutes().toString().padStart(2, "0")}
          </Text>
        </View>
      </View>
    );
  };

  return (
    <View className="flex-1 bg-gray-50">
      <FlatList
        contentContainerStyle={{
          flexGrow: 1,
          paddingVertical: 10,
          paddingHorizontal: 8,
        }}
        className="flex"
        data={loadedChat}
        keyExtractor={(item) => item.id}
        renderItem={renderMessageItem}
      />
      <KeyboardAvoidingView
        keyboardVerticalOffset={110}
        behavior="padding"
        className="p-3 bg-white border-t border-gray-300"
      >
        <SafeAreaView className="flex-row items-center gap-2">
          <TouchableOpacity
            onPress={handleImageUpload}
            disabled={isUploading}
            className="h-10 w-10 items-center justify-center"
          >
            {isUploading ? (
              <ActivityIndicator size="small" color="#0000ff" />
            ) : (
              <Text className="text-2xl">🖼️</Text>
            )}
          </TouchableOpacity>
          <TextInput
            className="flex-1 rounded-full h-10 px-4 bg-gray-100 border border-gray-300 focus:border-blue-500 focus:bg-white"
            value={message}
            onChangeText={setMessage}
            placeholder="Type a message..."
            textAlignVertical="center"
            onSubmitEditing={sendMessage}
          />
          <TouchableOpacity
            onPress={sendMessage}
            className="bg-blue-500 rounded-full h-10 w-10 items-center justify-center"
          >
            <Text className="text-white text-xl">↑</Text>
          </TouchableOpacity>
        </SafeAreaView>
      </KeyboardAvoidingView>
    </View>
  );
}
