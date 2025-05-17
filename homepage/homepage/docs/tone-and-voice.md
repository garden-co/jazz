# Jazz Documentation Tone and Voice Guide

## Core Principles

Write like you're helping a friend understand a new tool. Be clear, direct, and practical. Assume your reader is smart but busy.

## Voice

- **Direct**: Say what you mean simply
- **Practical**: Focus on getting things done
- **Friendly**: Write like you're talking to a friend
- **Knowledgeable**: Show depth through examples, not jargon
- **Progressive**: Build concepts step by step

## Do

- Start with working examples
- Add context naturally as needed
- Use active voice
- Break complex ideas into smaller chunks
- Let code do the heavy lifting
- Show common gotchas through examples

## Don't

- Explain what's obvious
- Use fancy technical terms when simple ones work
- Add artificial formality
- Repeat yourself
- State the obvious
- Hide key details in long paragraphs

## Examples

---

Instead of:

The CoValue instantiation process requires extending the CoMap class and defining appropriate schema fields utilizing the co namespace for type specification.

Write:

Create a CoValue by extending CoMap and defining your fields:

```ts
const Agent = co.map({
  name: co.string,
  status: co.string,
});
```

---

Instead of:

It is imperative to ensure proper cleanup of subscriptions to prevent memory leaks and unnecessary network connections in your application.

Write:

Remember to clean up subscriptions when you're done:

```ts
const unsubscribe = Company.subscribe(id, me, {...});
// Later:
unsubscribe();
```

## Code Examples

- Should be complete and runnable
- Start simple, add complexity gradually
- Include TypeScript types naturally
- Show real-world patterns
- Handle errors without being verbose
