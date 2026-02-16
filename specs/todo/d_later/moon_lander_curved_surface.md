# Moon lander: curved surface rendering

Render the moon ground as a gentle arc rather than a flat line, giving the visual impression of standing on a small spherical body. The world already wraps horizontally — this would make that wrapping feel natural.

## Approach

- Purely cosmetic — physics stays 1D horizontal with flat ground collision
- `drawGround()` in `render.ts` draws the surface as a downward-curving arc based on camera position
- Curvature radius should be tuned so the curve is subtle at walking scale but clearly visible during descent
- Deposits and the lander sit on the arc visually (offset their Y by the curve height at their X)
