"use client";

import { useState, useEffect } from "react";

const MINUTE = 60;
const HOUR = 3600;
const DAY = 86400;

function format(seconds: number): string {
  if (seconds < 10) return "just now";
  if (seconds < MINUTE) return `${Math.floor(seconds)}s ago`;
  if (seconds < HOUR) return `${Math.floor(seconds / MINUTE)}m ago`;
  if (seconds < DAY) return `${Math.floor(seconds / HOUR)}h ago`;
  const days = Math.floor(seconds / DAY);
  return days === 1 ? "yesterday" : `${days}d ago`;
}

function interval(seconds: number): number {
  if (seconds < MINUTE) return 10_000;
  if (seconds < HOUR) return 60_000;
  return 300_000;
}

interface Props {
  date: string | number | Date;
  className?: string;
}

export function RelativeTime({ date, className }: Props) {
  const target = new Date(date).getTime();
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const tick = () => setNow(Date.now());
    const elapsed = (Date.now() - target) / 1000;
    const id = setInterval(tick, interval(elapsed));
    return () => clearInterval(id);
  }, [target]);

  const elapsed = Math.max(0, (now - target) / 1000);

  return (
    <time
      dateTime={new Date(target).toISOString()}
      title={new Date(target).toLocaleString()}
      className={className}
    >
      {format(elapsed)}
    </time>
  );
}
