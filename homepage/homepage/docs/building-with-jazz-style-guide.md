# Building with Jazz Documentation Style Guide

## Purpose

This guide specifically addresses our approach to the **Building with Jazz** documentation section. This section is primarily focused on the **Explanation** quadrant of the [Diátaxis framework](https://diataxis.fr/), helping developers understand Jazz concepts and architecture before diving into implementation details.

With that in mind, we have to step away from our usual approach of code first, explain less, as these docs are intended to be conceptual. We only show minimal code in support of conveying an idea.

## Core Principles for Building with Jazz Docs

Our explanatory documentation helps developers understand what Jazz is, how it works, and why it matters. We write like we're helping a smart but busy friend understand the power and potential of Jazz—clear, direct, and focused on concepts rather than exhaustive implementation details.

## Voice and Tone

- **Conversational and enthusiastic**: Write as if explaining to a friend who's excited about collaborative applications
- **Direct and practical**: Focus on concepts and why they matter, not just technical details
- **Friendly and approachable**: Use analogies that make technical concepts relatable
- **Progressive**: Build understanding step by step, from simple to complex

## Content Structure for Explanation-Focused Docs

- **Focus on the "why"**: Explain concepts and their benefits before implementation details
- **Minimal code examples**: Show just enough code to illustrate the concept, not complete implementations
- **Link to deeper resources**: Point to reference documentation for implementation details
- **Use metaphors and analogies**: Make abstract concepts concrete with real-world comparisons
- **Progressive disclosure**: Start with the simplest explanation, then add complexity when needed

## Code Examples in Explanatory Documentation

- **Keep them minimal**: Show only what's needed to illustrate the concept
- **Focus on ideas, not implementation**: Highlight the conceptual pattern, not all the details
- **Add meaningful comments**: Use comments to explain the purpose, not the syntax
- **Progressive complexity**: If multiple examples are needed, start simple and build up
- **Link to complete examples**: Direct readers to full implementation examples in reference docs

## Formatting Guidelines

- **Use headings as signposts**: Make headings informative and conversational
- **Employ bullet points**: Break down complex ideas into digestible chunks
- **Bold key concepts**: Highlight important terms when first introduced
- **Keep paragraphs short**: Aim for 2-4 sentences maximum
- **Use numbered lists for sequences**: When explaining steps or related concepts

## Examples of Good Building with Jazz Documentation

### Instead of:

```
The Account class is a specialized schema that contains profile and root nodes. Profile nodes are public data structures while root nodes are private. This distinction enables effective data access control.
```

### Write:

```
Think of Accounts as your users' digital identities in Jazz. They're the magical starting point that connects people to all their data.

Every Account has two key parts:

1. **Profile**: The public face of your user that everyone can see (think username and avatar)
2. **Root**: Their private data vault that only they can access (like notes and settings)

This clever split makes it super easy to manage both public and private data.
```

### Instead of:

```
Implement migrations to handle schema evolution by extending the account schema and implementing the migration method with transformation logic:

[complex code example with many details]
```

### Write:

````
Apps change over time, and your account structure needs to keep up. Jazz makes this painless with migrations that run automatically when users log in:

```ts
// Add migration support to handle schema changes
MyAppAccount.withMigration((account) => {
  // Add new fields or update existing data
  if (!account.root?.bookmarks) {
    account.root.bookmarks = co.list(Bookmark).create();
  }
});
```

The best part? You don't need to worry about database migrations or complicated update scripts. Jazz handles it all automatically.

````

## Balancing Explanation with Other Documentation Types

The Building with Jazz section focuses on explanation, but connects to other documentation types:

- **Link to Tutorials**: "To see this in action, check out our [Getting Started Tutorial]"
- **Reference Related How-To Guides**: "For specific implementation steps, see [How to Implement Account Migrations]"
- **Connect to Reference Docs**: "For the complete API, visit the [Accounts API Reference]"

This balanced approach ensures developers can both understand concepts and find implementation details when needed.

## Writing Process for Building with Jazz Docs

1. **Understand the concept deeply**: Make sure you fully grasp what you're explaining
2. **Start with a metaphor or analogy**: How would you explain this to a non-technical friend?
3. **Outline key benefits**: Why should the reader care about this concept?
4. **Introduce minimal code**: What's the simplest code snippet that illustrates the idea?
5. **Link to details**: Where can readers find implementation details?
6. **Revise for simplicity**: Can any technical jargon be replaced with simpler terms?

Remember that in the Building with Jazz section, we're focused on building understanding, not just listing features or providing step-by-step instructions. The goal is to help developers grasp the concepts and architecture that make Jazz powerful, setting them up for success when they move to implementation.
