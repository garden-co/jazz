export type Vec2 = {
  x: number;
  y: number;
};

export type Cursor = {
  position: Vec2;
};

export type Camera = {
  position: Vec2;
};

export type RemoteCursor = Cursor & {
  id: ID;
  color: string;
  name: string;
  isRemote: true;
  isDragging: boolean;
};

export type ViewBox = {
  x: number;
  y: number;
  width: number;
  height: number;
};
