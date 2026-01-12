import React, { useEffect, useState } from "react";
import {
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  ScrollView,
  Alert,
  useColorScheme,
} from "react-native";
import { co, Group } from "jazz-tools";
import { useAccount, useCoState, useLogOut } from "jazz-tools/react-native";
import { useIsAuthenticated } from "jazz-tools/react-core";
import { Note, NoteList } from "./schema";

type Props = {
  navigation: any;
};

/**
 * A simple notes screen demonstrating Jazz CRUD operations.
 * Notes are synced in real-time across all devices.
 */
export function NotesScreen({ navigation }: Props) {
  const colorScheme = useColorScheme();
  const darkMode = colorScheme === "dark";
  const [newNoteTitle, setNewNoteTitle] = useState("");
  const [noteListId, setNoteListId] = useState<string | null>(null);

  const me = useAccount();
  const logOut = useLogOut();
  const isAuthenticated = useIsAuthenticated();

  // Navigate back to Auth when logged out
  useEffect(() => {
    if (!isAuthenticated) {
      navigation.replace("Auth");
    }
  }, [isAuthenticated, navigation]);

  // Create a note list when the user is loaded
  useEffect(() => {
    if (me?.$isLoaded) {
      // Create a collaborative group and note list
      const group = Group.create({ owner: me });
      group.addMember("everyone", "writer");
      const notes = NoteList.create([], { owner: group });
      setNoteListId(notes.$jazz.id);
    }
  }, [me?.$isLoaded]);

  const noteList = useCoState(NoteList, noteListId ?? undefined, {
    resolve: { $each: { title: true, content: true } },
  });

  const handleAddNote = () => {
    if (!noteList?.$isLoaded || !newNoteTitle.trim()) return;

    const note = Note.create(
      {
        title: co.plainText().create(newNoteTitle.trim(), noteList.$jazz.owner),
        content: co.plainText().create("", noteList.$jazz.owner),
      },
      noteList.$jazz.owner,
    );

    noteList.$jazz.push(note);
    setNewNoteTitle("");
  };

  const handleDeleteNote = (index: number) => {
    if (!noteList?.$isLoaded) return;
    Alert.alert("Delete Note", "Are you sure?", [
      { text: "Cancel", style: "cancel" },
      {
        text: "Delete",
        style: "destructive",
        onPress: () => noteList.$jazz.splice(index, 1),
      },
    ]);
  };

  const handleLogOut = () => {
    Alert.alert("Log Out", "Are you sure you want to log out?", [
      { text: "Cancel", style: "cancel" },
      { text: "Log Out", style: "destructive", onPress: logOut },
    ]);
  };

  const profile = me?.$isLoaded ? me.profile : null;

  return (
    <View style={[styles.container, darkMode ? styles.darkBg : styles.lightBg]}>
      <View style={styles.header}>
        <View>
          <Text
            style={[
              styles.headerTitle,
              darkMode ? styles.darkText : styles.lightText,
            ]}
          >
            My Notes
          </Text>
          {profile?.$isLoaded && (
            <Text
              style={[
                styles.headerSubtitle,
                darkMode ? styles.darkSubtext : styles.lightSubtext,
              ]}
            >
              Signed in as {profile.name}
            </Text>
          )}
        </View>
        <TouchableOpacity onPress={handleLogOut} style={styles.logoutButton}>
          <Text style={styles.logoutButtonText}>Log Out</Text>
        </TouchableOpacity>
      </View>

      <View style={styles.addNoteContainer}>
        <TextInput
          style={[
            styles.addNoteInput,
            darkMode ? styles.darkInput : styles.lightInput,
          ]}
          placeholder="New note title..."
          placeholderTextColor={darkMode ? "#888" : "#666"}
          value={newNoteTitle}
          onChangeText={setNewNoteTitle}
          onSubmitEditing={handleAddNote}
        />
        <TouchableOpacity
          style={[
            styles.addButton,
            !newNoteTitle.trim() && styles.disabledButton,
          ]}
          onPress={handleAddNote}
          disabled={!newNoteTitle.trim()}
        >
          <Text style={styles.addButtonText}>Add</Text>
        </TouchableOpacity>
      </View>

      <ScrollView style={styles.notesList}>
        {noteList?.$isLoaded && noteList.length === 0 && (
          <Text
            style={[
              styles.emptyText,
              darkMode ? styles.darkSubtext : styles.lightSubtext,
            ]}
          >
            No notes yet. Add one above!
          </Text>
        )}
        {noteList?.$isLoaded &&
          noteList.map((note, index) => (
            <TouchableOpacity
              key={note.$jazz.id}
              style={[
                styles.noteItem,
                darkMode ? styles.darkCard : styles.lightCard,
              ]}
              onLongPress={() => handleDeleteNote(index)}
            >
              <Text
                style={[
                  styles.noteTitle,
                  darkMode ? styles.darkText : styles.lightText,
                ]}
              >
                {note.title?.toString() || "Untitled"}
              </Text>
              <Text
                style={[
                  styles.noteHint,
                  darkMode ? styles.darkSubtext : styles.lightSubtext,
                ]}
              >
                Long press to delete
              </Text>
            </TouchableOpacity>
          ))}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  darkBg: {
    backgroundColor: "#000",
  },
  lightBg: {
    backgroundColor: "#f5f5f5",
  },
  darkText: {
    color: "#fff",
  },
  lightText: {
    color: "#000",
  },
  darkSubtext: {
    color: "#888",
  },
  lightSubtext: {
    color: "#666",
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    padding: 16,
    paddingTop: 60,
  },
  headerTitle: {
    fontSize: 24,
    fontWeight: "bold",
  },
  headerSubtitle: {
    fontSize: 14,
    marginTop: 4,
  },
  logoutButton: {
    padding: 8,
  },
  logoutButtonText: {
    color: "#ff3b30",
    fontSize: 16,
  },
  addNoteContainer: {
    flexDirection: "row",
    padding: 16,
    gap: 12,
  },
  addNoteInput: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 8,
    padding: 12,
    fontSize: 16,
  },
  darkInput: {
    borderColor: "#333",
    backgroundColor: "#1a1a1a",
    color: "#fff",
  },
  lightInput: {
    borderColor: "#ddd",
    backgroundColor: "#fff",
    color: "#000",
  },
  addButton: {
    backgroundColor: "#007aff",
    paddingHorizontal: 20,
    borderRadius: 8,
    justifyContent: "center",
  },
  addButtonText: {
    color: "#fff",
    fontSize: 16,
    fontWeight: "600",
  },
  disabledButton: {
    opacity: 0.5,
  },
  notesList: {
    flex: 1,
    padding: 16,
  },
  emptyText: {
    textAlign: "center",
    fontSize: 16,
    marginTop: 32,
  },
  noteItem: {
    padding: 16,
    borderRadius: 8,
    marginBottom: 12,
  },
  darkCard: {
    backgroundColor: "#1a1a1a",
  },
  lightCard: {
    backgroundColor: "#fff",
  },
  noteTitle: {
    fontSize: 16,
    fontWeight: "500",
  },
  noteHint: {
    fontSize: 12,
    marginTop: 4,
  },
});
