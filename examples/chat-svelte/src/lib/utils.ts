import { co, type Group } from 'jazz-tools';
import { createImage } from 'jazz-tools/media';
import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { FileAttachment, ImageAttachment } from '@/lib/schema';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export type WithElementRef<T, U extends HTMLElement = HTMLElement> = T & {
  ref?: U | null;
};

/** Omit `child` from props (used by bits-ui/shadcn components). */
export type WithoutChild<T> = Omit<T, 'child'>;

/** Omit `children` and `child` from props (used by bits-ui/shadcn components). */
export type WithoutChildrenOrChild<T> = Omit<T, 'children' | 'child'>;

const animals = [
  'elephant',
  'penguin',
  'giraffe',
  'octopus',
  'kangaroo',
  'dolphin',
  'cheetah',
  'koala',
  'platypus',
  'pangolin',
  'rhinoceros',
  'zebra',
  'lion',
  'tiger',
  'otter',
  'sloth',
  'capybara',
  'quokka',
  'lemur',
  'meerkat',
  'wombat',
  'hedgehog',
  'armadillo',
  'seal',
  'manatee',
  'narwhal',
  'beluga',
  'orca',
  'walrus',
  'fox',
  'alpaca',
  'llama',
  'tapir',
  'okapi'
];

export function getRandomUsername(): string {
  return `Anonymous ${animals[Math.floor(Math.random() * animals.length)]}`;
}

export const inIframe = typeof window !== 'undefined' && window.self !== window.top;

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function formatBytes(fileSize: number) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (fileSize >= 1024 && i < units.length - 1) {
    fileSize /= 1024;
    i++;
  }
  return `${fileSize.toFixed(2)} ${units[i]}`;
}

export async function uploadFile(
  file: File,
  {
    onProgress,
    owner
  }: {
    onProgress?: (progress: number) => void;
    owner?: Group;
  }
) {
  let group: Group | undefined;
  if (owner) {
    group = co.group().create();
    group.addMember(owner, 'writer');
  }
  if (file.type.startsWith('image/')) {
    const img = await createImage(file, {
      placeholder: 'blur',
      progressive: true,
      owner: group
    });
    const attachment = ImageAttachment.create(
      {
        type: 'image',
        attachment: img,
        name: file.name
      },
      { owner: group }
    );
    return attachment;
  }

  const fileAttachment = await co.fileStream().createFromBlob(file, {
    onProgress: (p) => onProgress?.(Math.round(p * 100)),
    owner: group
  });
  const attachment = FileAttachment.create(
    {
      type: 'file',
      attachment: fileAttachment,
      name: file.name
    },
    { owner: group }
  );
  return attachment;
}
