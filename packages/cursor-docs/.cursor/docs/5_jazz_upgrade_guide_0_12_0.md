# **Jazz 0.12.0 - Deeply Resolved Data**

## **Overview**
Jazz 0.12.0 improves loading nested data with type-safe queries, better permission checks, and graceful handling of missing data.

## **Breaking Changes**

### **1. New Resolve API**
More TypeScript-friendly API for deep loading:

```typescript
// Before
const { me } = useAccount({
  root: {
    friends: []
  }
});

// After
const { me } = useAccount({
  resolve: {
    root: {
      friends: true
    }
  }
});
```

### **2. Collections Use `true` Instead of `[]` or `{}`**
```typescript
// Before - using empty arrays/objects
const shallowData = useCoState(MyClass, id, []);

// After - using true
const shallowData = useCoState(MyClass, id, { resolve: true });
```

### **3. Deep Resolving with `$each`**
For `CoList`:
```typescript
class ListOfTasks extends CoList.Of(co.ref(Task)) {}

// Before
const tasks = useCoState(ListOfTasks, id, [{}]);

// After
const tasks = useCoState(ListOfTasks, id, {
  resolve: { $each: true }
});
```

For `CoMap.Record`:
```typescript
class UsersByUsername extends CoMap.Record(co.ref(MyAppAccount)) {}

// Before
const usersByUsername = useCoState(UsersByUsername, id, [{}]);

// After
const usersByUsername = useCoState(UsersByUsername, id, {
  resolve: { $each: true }
});
```

### **4. Nested Loading Syntax**
```typescript
// Before
const tasksWithAssigneesAndTheirOrgs = useCoState(ListOfTasks, id, [{
  assignees: [{ org: {}}]
}]);

// After
const tasksWithAssigneesAndTheirOrgs = useCoState(ListOfTasks, id, {
  resolve: {
    $each: {
      assignees: {
        $each: { org: true }
      }
    }
  }
});
```

### **5. Consistent Load Options**
```typescript
// Before
Playlist.load(id, otherAccount, {
  tracks: [],
});

// After
Playlist.load(id, {
  loadAs: otherAccount,
  resolve: { tracks: true }
});
```

### **6. Improved Permission Checks**
More clear handling of loading/missing states:
```typescript
class ListOfTracks extends CoList.Of(co.optional.ref(Track)) {}

// Before (ambiguous states)
const value = useCoState(ListOfTasks, id, [{}]);
if (value === undefined) return <div>Loading or access denied</div>;
if (value === null) return <div>Not found</div>;

// After
const value = useCoState(ListOfTasks, id, { resolve: { $each: true } });
if (value === undefined) return <div>Loading...</div>;
if (value === null) return <div>Not found or access denied</div>;

// This will only show tracks that we have access to and that are loaded
return tracks.map(track => track && <MusicTrack track={track} />);
```

## **New Features**

### **The Resolved Type Helper**
Define expected deeply loaded data:
```typescript
type PlaylistResolved = Resolved<Playlist, {
  tracks: { $each: true }
}>;

function TrackListComponent({ playlist }: { playlist: PlaylistResolved }) {
  // Safe access to resolved tracks
  return playlist.tracks.map(track => /* ... */);
}
```

### **Key Takeaways**
- Replace array/object queries with `{ resolve: ... }` pattern
- Use `true` for shallow loading, `$each` for collections
- Improved permission checks: `undefined` = loading, `null` = missing/denied
- `Resolved<T>` type helper ensures type-safe access to loaded data
- More consistent API across all loading methods