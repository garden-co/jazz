import { co, z, Group } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
const colleagueAccount = await createJazzTestAccount();

// #region Basic
const note = co.plainText().create("Meeting notes");

// Update the text
note.$jazz.applyDiff("Meeting notes for Tuesday");

console.log(note.toString()); // "Meeting notes for Tuesday"
// #endregion

// #region Comparison
const Profile = co.profile({
  name: z.string(),
  bio: co.plainText(), // Plain text field
  description: co.richText(), // Rich text with formatting
});
// #endregion

// #region StringInitial
// Create plaintext with default ownership (current user)
const meetingNotes = co.plainText().create("Meeting notes");

// Create rich text with HTML content
const document = co
  .richText()
  .create("<p>Project <strong>overview</strong></p>");
// #endregion

// #region Ownership
// Create with shared ownership
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

const teamNote = co.plainText().create("Team updates", { owner: teamGroup });
// #endregion

// #region Reading
// Get the text content
console.log(note.toString()); // "Meeting notes"
console.log(`${note}`); // "Meeting notes"

// Check the text length
console.log(note.length); // 14
// #endregion

// #region Editing
// Insert text at a specific position
note.insertBefore(8, "weekly "); // "Meeting weekly notes"

// Insert after a position
note.insertAfter(21, " for Monday"); // "Meeting weekly notes for Monday"

// Delete a range of text
note.deleteRange({ from: 8, to: 15 }); // "Meeting notes for Monday"

// Apply a diff to update the entire text
note.$jazz.applyDiff("Team meeting notes for Tuesday");
// #endregion

// #region ApplyDiff
// Original text: "Team status update"
const minutes = co.plainText().create("Team status update");

// Replace the entire text with a new version
minutes.$jazz.applyDiff("Weekly team status update for Project X");

// Make partial changes
let text = minutes.toString();
text = text.replace("Weekly", "Monday");
minutes.$jazz.applyDiff(text); // Efficiently updates only what changed
// #endregion
