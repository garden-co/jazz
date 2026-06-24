import { useDb } from "jazz-tools/react";
import { app, type Room } from "../../schema.js";

const languages = ["plaintext", "javascript", "typescript", "python", "rust", "json", "markdown"];

type LanguagePickerProps = {
  room: Room;
};

export function LanguagePicker({ room }: LanguagePickerProps) {
  const db = useDb();

  return (
    <label>
      Language{" "}
      <select
        value={room.editorLanguage}
        onChange={(event) => {
          db.update(app.rooms, room.id, { editorLanguage: event.target.value });
        }}
      >
        {languages.map((language) => (
          <option key={language} value={language}>
            {language}
          </option>
        ))}
      </select>
    </label>
  );
}
