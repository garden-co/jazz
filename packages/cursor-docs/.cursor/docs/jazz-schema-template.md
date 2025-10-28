# Jazz Schema Template

This template provides a comprehensive example of how to structure Jazz schemas for collaborative applications. Use this as a reference when creating your own schemas.

## Basic Schema Structure

```typescript
import { Group, co, z } from "jazz-tools";
import { CustomType1, CustomType2 } from "./types";

// 1. Define Feeds for real-time collaborative data
export const ItemFeed = co.feed(CustomType1);

// 2. Define Profile schema for user information
export const UserProfile = co.profile({
  name: z.string(),
  email: z.string().optional(),
  avatar: z.string().optional(),
  // Add other profile fields as needed
});

// 3. Define Root schema for main application data
export const AppRoot = co.map({
  // Core application state
  mainData: CustomType2,
  // Real-time collaborative feeds
  items: ItemFeed,
  // Additional application-specific fields
  settings: co.map({
    theme: z.enum(["light", "dark"]).optional(),
    language: z.string().optional(),
  }).optional(),
});

// 4. Define Container schemas for organizing data
export const DataContainer = co.map({
  itemFeed: ItemFeed,
  metadata: co.map({
    createdAt: z.string(),
    updatedAt: z.string(),
  }).optional(),
});

// 5. Define the main Account schema with migration logic
export const AppAccount = co
  .account({
    profile: UserProfile,
    root: AppRoot,
  })
  .withMigration((account) => {
    // Initialize root if it doesn't exist
    if (!account.$jazz.has("root")) {
      account.root = AppRoot.create({
        mainData: {
          // Initialize with default values
          // Customize based on your application needs
        },
        items: ItemFeed.create([]),
        settings: {
          theme: "light",
          language: "en",
        },
      });
    }

    // Initialize profile if it doesn't exist
    if (!account.$jazz.has("profile")) {
      const group = Group.create();
      group.addMember("everyone", "reader"); // Profile visible to everyone
      
      account.profile = UserProfile.create(
        {
          name: "Anonymous User",
          // Add default profile values
        },
        group,
      );
    }
  });
```

## Schema Patterns and Best Practices

### 1. Naming Conventions
- Use PascalCase for schema names: `UserProfile`, `AppRoot`
- Use descriptive names that reflect the data structure
- Suffix feeds with "Feed": `MessageFeed`, `CommentFeed`
- Suffix containers with "Container": `ChatContainer`

### 2. Data Organization
- **Feeds**: Use for real-time collaborative lists (messages, comments, items)
- **Maps**: Use for structured data with known fields
- **Profiles**: Use for user-specific information
- **Root**: Use for main application state

### 3. Migration Patterns
```typescript
.withMigration((account) => {
  // Always check if data exists before creating
  if (!account.$jazz.has("root")) {
    account.root = AppRoot.create({
      // Initialize with sensible defaults
    });
  }
  
  if (!account.$jazz.has("profile")) {
    const group = Group.create();
    group.addMember("everyone", "reader");
    
    account.profile = UserProfile.create({
      // Default profile data
    }, group);
  }
})
```

### 4. Permission Patterns
```typescript
// Public data (readable by everyone)
const group = Group.create();
group.addMember("everyone", "reader");

// Private data (only owner can access)
// Don't add any members to the group

// Collaborative data (specific users can edit)
const group = Group.create();
group.addMember("user123", "writer");
group.addMember("user456", "reader");
```

### 5. Common Schema Types

#### For Chat Applications:
```typescript
export const Message = co.map({
  content: z.string(),
  timestamp: z.string(),
  authorId: z.string(),
});

export const MessageFeed = co.feed(Message);

export const ChatRoot = co.map({
  messages: MessageFeed,
  participants: co.list(z.string()),
});
```

#### For Todo Applications:
```typescript
export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
  dueDate: z.string().optional(),
  priority: z.enum(["low", "medium", "high"]).optional(),
});

export const TodoFeed = co.feed(TodoItem);

export const TodoRoot = co.map({
  todos: TodoFeed,
  categories: co.list(z.string()),
});
```

#### For Document Collaboration:
```typescript
export const DocumentVersion = co.map({
  content: z.string(),
  version: z.number(),
  timestamp: z.string(),
  authorId: z.string(),
});

export const DocumentFeed = co.feed(DocumentVersion);

export const DocumentRoot = co.map({
  currentContent: z.string(),
  versions: DocumentFeed,
  collaborators: co.list(z.string()),
});
```

## Usage Guidelines

1. **Start Simple**: Begin with basic maps and add complexity as needed
2. **Plan Permissions**: Consider who needs access to what data
3. **Use Migrations**: Always include migration logic for schema evolution
4. **Type Safety**: Leverage TypeScript and Zod for robust type checking
5. **Performance**: Use feeds for frequently updated data, maps for stable data

## Common Pitfalls to Avoid

1. **Don't** create deeply nested structures - keep schemas flat when possible
2. **Don't** forget migration logic - it's essential for schema evolution
3. **Don't** mix different data types in the same feed
4. **Don't** create circular references between schemas
5. **Don't** forget to set appropriate permissions for sensitive data 