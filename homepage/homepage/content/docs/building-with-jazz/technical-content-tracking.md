# Technical Content for Reference Documentation

This document tracks content that was removed from the explanation-focused "Building with Jazz" section but should be preserved for technical reference documentation.

## Schemas

### Removed Content
- Detailed information about specific CoValue types
- In-depth explanation of field types and references
- Code examples showing how to create and manipulate schema instances
- Information about SchemaUnions and their usage
- Examples of computed fields and methods
- Specific details about optional fields and refs

### Source Location
Original content from: `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/schemas/covalues.mdx`

## Providers

### Removed Content
- Detailed provider configuration options:
  ```tsx
  // Configure how your app connects to the Jazz network
  const syncConfig = {
    // Connect to Jazz Cloud (or your own sync server)
    peer: "wss://cloud.jazz.tools/?key=your-api-key",
    
    // When to sync: "always" (default), "never", or "signedUp"
    when: "always"
  }
  ```
- Account Schema specific configuration details
- Section on provider options including:
  - Guest mode configuration
  - Default profile name setting
  - Logout handling
  - Anonymous account data handling
- References to authentication states and how they affect synchronization

### Source Location
Original content from: `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/project-setup/providers/react.mdx`

## Authentication

### Removed Content
- Detailed code examples showing implementation of authentication state detection
- Specific code for migrating data from anonymous to authenticated accounts
- Configuration code examples for providers related to authentication
- Implementation details for controlling sync in different authentication states
- Code examples for guest mode configuration

### Source Location
Original content from: `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/authentication/overview.mdx` and `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/authentication/authentication-states.mdx`

Most of the conceptual content about authentication states was borrowed from authentication-states.mdx. Options for handling this duplication:

1. **Remove the conceptual sections**: Remove overlapping conceptual content from authentication-states.mdx, leaving only implementation details and code examples. Add a prominent link at the top directing users to the explanation document for conceptual understanding.

2. **Keep both but cross-reference**: Keep the original content but add clear cross-references between the documents, with explanation document for concepts and the original for implementation details.

3. **Complete reorganization**: Completely reorganize authentication-states.mdx to focus solely on implementation, moving all conceptual content to the explanation document.

For now, we're maintaining both documents but should decide on an approach when finalizing the documentation structure.

## Groups and Ownership

### Removed Content
- Detailed implementation code examples for adding members to groups
- Code examples for public sharing and invite links
- Common Group Patterns section with examples for:
  - Organization Structure
  - Project Collaboration
  - Public Community

Note: The Common Group Patterns section contained valuable examples showing real-world usage patterns. This content should be preserved and potentially added to a patterns guide, cookbook, or examples section in the reference documentation.

### Source Location
Original content from: `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/groups/intro.mdx`, `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/groups/sharing.mdx`, and `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/groups/inheritance.mdx`

## Sync and Storage

### Removed Content
- Detailed instructions for using Jazz Cloud with API key examples
- Command line instructions for running a self-hosted sync server
- Command line options and parameters for configuring a sync server
- References to the source code and GitHub repositories
- Configuring Sync in Your Application section with code examples
- Offline-First Approach section explaining offline capabilities
- Sync and Authentication section explaining how sync relates to authentication states

### Source Location
Original content from: `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/sync-and-storage.mdx`

### Technical Documentation Needed
We should create a detailed technical reference document for sync server configuration that covers:
- Complete configuration options for self-hosted sync servers
- Performance tuning parameters
- Security considerations
- Deployment scenarios and best practices
- Monitoring and maintenance

This would be valuable for users who need to self-host their sync server with specific configuration requirements.

## Server Workers

### Removed Content
- Code example for generating Server Worker credentials
- Instructions for storing credentials as environment variables
- Code example for starting a Server Worker
- Implementation details for loading/subscribing to CoValues
- Specific implementation patterns with code examples

### Source Location
Original content from: `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/project-setup/server-side.mdx` and `/Users/benjamin/projects/gcmp/jazz/homepage/homepage/content/docs/building-with-jazz/server-workers.mdx`

## Next Steps

### Missing Topics to Address
- ~~**Data Loading and Subscriptions**~~: Rather than creating a separate document, we've incorporated this topic into existing documents:
  - Added a substantial section about subscriptions and deep loading to the **Providers** document
  - Added an explanation of accessing shared data to the **Groups** document
  - Added links to the comprehensive [Subscription and Loading](/docs/react/using-covalues/subscription-and-loading) documentation in both documents

This approach ensures that data loading concepts are covered in relevant contexts rather than isolated in a separate document.

We've updated all Next Steps sections to reflect the current document flow:
1. Installation → Schemas
2. Schemas → Providers
3. Providers → Accounts
4. Accounts → Authentication
5. Authentication → Groups
6. Groups → Sync
7. Sync → Server Workers
8. Server Workers → (end of journey)

As we continue creating explanation-focused content for the "Building with Jazz" section, we should add to this document to ensure all technical reference material is preserved for the appropriate documentation.
