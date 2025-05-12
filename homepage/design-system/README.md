# Jazz Design System

## Getting Started

First, install packages

```bash
pnpm i
```

Then run the development server:

```bash
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

This project uses [`next/font`](https://nextjs.org/docs/basic-features/font-optimization) to automatically optimize and load Inter, a custom Google Font.

The design sysem is created to enable developers to create apps and components quickly, which have baked in design decisions, allowing you to focus on the code.

## Design Tokens

The design system purposefully reduces the number of decisions a designer needs to make, mainly reducing design decisions to whether something is:

- `default` - the default appearance
- `highlight` - a more prominent appearance
- `muted` - a less prominent appearance

These tokens have baked in `light` & `dark` modes, so the colour reacts to be less/more prominent corresponding to the background colour.

They also handle `text` and `background` colours interchangably, this means a `text-highlight` is not the same colour as a `bg-highlight` so can be used correspondingly, for example to create double prominence on text, you can make the text 'bolder' and the background a transparent `primary` colour, to really emphasise text, the same applies to `muted`, which would have a similar but opposite effect.

The design system is centered around the variable `primary`, with the `secondary` variable and some `highlight` variables being created from this single variable, this allows the entire theme to be quickly updated by changing this colour, converting the entire theme to a harmonious colour palette.