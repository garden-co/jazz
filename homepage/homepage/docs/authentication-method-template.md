# Authentication Method Template

Use this template structure for all authentication method documentation pages to ensure consistency.

````markdown
export const metadata = { title: "[Method] Authentication" };

import { CodeGroup, ContentByFramework } from "@/components/forMdx";

# [Method] Authentication

Brief introduction explaining what this authentication method is.

## How it works

Clear, simple explanation of the mechanism behind this auth method.

## Key benefits

- Benefit one
- Benefit two
- Benefit three
- Benefit four

## Implementation

<ContentByFramework framework="react">
<CodeGroup>
```tsx twoslash
// Code example here
```

</CodeGroup>
</ContentByFramework>

## Examples

Links to live demos using this authentication method.

## When to use [Method]

Guidelines about when this method is most appropriate.

## Limitations and considerations

Any drawbacks or special considerations to be aware of.

## Additional resources (optional)

External links and further reading where applicable.
````
