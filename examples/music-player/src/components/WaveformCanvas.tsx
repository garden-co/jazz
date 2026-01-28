"use client";

import { MusicTrack, MusicTrackWaveform } from "@/1_schema";
import { AudioManager, useAudioManager } from "@/lib/audio/AudioManager";
import { cn } from "@/lib/utils";
import type React from "react";

import { useEffect, useRef, useSyncExternalStore } from "react";

type Props = {
  track: MusicTrack;
  height?: number;
  barColor?: string;
  progressColor?: string;
  backgroundColor?: string;
  className?: string;
};

const DEFAULT_HEIGHT = 96;

// Downsample PCM into N peaks (abs max in window)
function buildPeaks(channelData: number[], samples: number): Float32Array {
  const length = channelData.length;
  if (channelData.length < samples) {
    // Create a peaks array that interpolates the channelData
    const interpolatedPeaks = new Float32Array(samples);
    for (let i = 0; i < samples; i++) {
      const index = Math.floor(i * (length / samples));
      interpolatedPeaks[i] = channelData[index];
    }
    return interpolatedPeaks;
  }

  const blockSize = Math.floor(length / samples);
  const peaks = new Float32Array(samples);

  for (let i = 0; i < samples; i++) {
    const start = i * blockSize;
    let end = start + blockSize;
    if (end > length) end = length;
    let max = 0;
    for (let j = start; j < end; j++) {
      const v = Math.abs(channelData[j]);
      if (v > max) max = v;
    }
    peaks[i] = max;
  }
  return peaks;
}

type DrawWaveformCanvasProps = {
  canvas: HTMLCanvasElement;
  waveformData: number[] | undefined;
  duration: number;
  currentTime: number;
  barColor?: string;
  progressColor?: string;
  backgroundColor?: string;
  isAnimating: boolean;
  animationProgress: number;
  progress: number;
};

function drawWaveform(props: DrawWaveformCanvasProps) {
  const {
    canvas,
    waveformData,
    isAnimating,
    animationProgress,
    barColor = "hsl(215, 16%, 47%)",
    progressColor = "hsl(142, 71%, 45%)",
    backgroundColor = "transparent",
    progress,
  } = props;

  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const dpr = window.devicePixelRatio || 1;
  const cssWidth = canvas.clientWidth;
  const cssHeight = canvas.clientHeight;
  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  ctx.scale(dpr, dpr);

  // Background
  ctx.fillStyle = backgroundColor;
  ctx.fillRect(0, 0, cssWidth, cssHeight);

  if (!waveformData || !waveformData.length) {
    // Draw placeholder line
    ctx.strokeStyle = "hsl(215, 20%, 65%)";
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(0, cssHeight / 2);
    ctx.lineTo(cssWidth, cssHeight / 2);
    ctx.stroke();
    return;
  }

  const midY = cssHeight / 2;
  const barWidth = 2; // px
  const gap = 1;
  const totalBars = Math.floor(cssWidth / (barWidth + gap));
  const ds = buildPeaks(waveformData, totalBars);

  const draw = (color: string, untilBar: number, start = 0) => {
    ctx.fillStyle = color;
    for (let i = start; i < untilBar; i++) {
      const v = ds[i] || 0;
      const h = Math.max(2, v * (cssHeight - 8)); // margin
      const x = i * (barWidth + gap);

      // Apply staggered animation
      if (isAnimating) {
        const barProgress = Math.max(0, Math.min(1, animationProgress / 0.2));
        const animatedHeight = h * barProgress;

        ctx.globalAlpha = barProgress;
        ctx.fillRect(x, midY - animatedHeight / 2, barWidth, animatedHeight);
      } else {
        ctx.fillRect(x, midY - h / 2, barWidth, h);
      }
    }
  };

  // Progress overlay
  const progressBars = Math.floor(
    totalBars * Math.max(0, Math.min(1, progress || 0)),
  );
  draw(progressColor, progressBars);
  // Base waveform
  draw(barColor, totalBars, progressBars);
}

type WaveformCanvasProps = {
  audioManager: AudioManager;
  canvas: HTMLCanvasElement;
  waveformId: string;
  duration: number;
  barColor?: string;
  progressColor?: string;
  backgroundColor?: string;
};

async function renderWaveform(props: WaveformCanvasProps) {
  const { audioManager, canvas, waveformId, duration } = props;

  let mounted = true;
  let currentTime = audioManager.currentTime;
  let waveformData: undefined | number[] = undefined;
  let isAnimating = true;
  const startTime = performance.now();
  let animationProgress = 0;
  const animationDuration = 800;

  function draw() {
    const progress = currentTime / duration;

    drawWaveform({
      canvas,
      waveformData,
      duration,
      currentTime,
      isAnimating,
      animationProgress,
      progress,
    });
  }

  const animate = (currentTime: number) => {
    if (!mounted) return;

    const elapsed = currentTime - startTime;
    animationProgress = Math.min(elapsed / animationDuration, 1);

    if (animationProgress < 1) {
      requestAnimationFrame(animate);
    } else {
      isAnimating = false;
    }

    draw();
  };

  requestAnimationFrame(animate);

  const unsubscribeFromCurrentTime = audioManager.on("timeUpdate", () => {
    currentTime = audioManager.currentTime;
    draw();
  });

  const unsubscribeFromWaveform = MusicTrackWaveform.subscribe(
    waveformId,
    {},
    (newResult) => {
      waveformData = newResult.data;
      draw();
    },
  );

  return () => {
    mounted = false;
    unsubscribeFromCurrentTime();
    unsubscribeFromWaveform();
  };
}

export default function WaveformCanvas({
  track,
  height = DEFAULT_HEIGHT,
  barColor, // muted-foreground-ish
  progressColor, // green
  backgroundColor,
  className,
}: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const audioManager = useAudioManager();

  const duration = track.duration;
  const waveformId = track.$jazz.refs.waveform?.id;

  // Animation effect
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    if (!waveformId) return;

    renderWaveform({
      audioManager,
      canvas,
      waveformId,
      duration,
      barColor,
      progressColor,
      backgroundColor,
    });
  }, [audioManager, canvasRef, waveformId, duration]);

  const onPointer = (e: React.PointerEvent<HTMLCanvasElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const ratio = (e.clientX - rect.left) / rect.width;
    const time = Math.max(0, Math.min(1, ratio)) * duration;
    audioManager.seekTo(time);
  };

  const currentTime = useSyncExternalStore(
    (callback) => audioManager.on("timeUpdate", callback),
    () => audioManager.currentTime,
  );
  const progress = currentTime / duration;

  return (
    <div className={cn("w-full", className)}>
      <div
        className="w-full rounded-md bg-background"
        style={{ height }}
        role="slider"
        aria-label="Waveform scrubber"
        aria-valuenow={Math.round((progress || 0) * 100)}
        aria-valuemin={0}
        aria-valuemax={100}
      >
        <canvas
          ref={canvasRef}
          className="w-full h-full rounded-md cursor-pointer"
          onPointerDown={onPointer}
          onPointerMove={(e) => {
            if (e.buttons === 1) onPointer(e);
          }}
        />
      </div>
    </div>
  );
}
