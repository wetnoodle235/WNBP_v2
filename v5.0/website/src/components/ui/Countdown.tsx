"use client";

import { useEffect, useState } from "react";

interface CountdownProps {
  /** ISO date string of the target time */
  target: string;
  /** Label shown above the countdown */
  label?: string;
  /** Callback when countdown reaches zero */
  onComplete?: () => void;
  /** Compact mode — single line */
  compact?: boolean;
}

interface TimeLeft {
  days: number;
  hours: number;
  minutes: number;
  seconds: number;
  total: number;
}

function calcTimeLeft(target: string): TimeLeft {
  const total = Math.max(0, new Date(target).getTime() - Date.now());
  return {
    total,
    days: Math.floor(total / (1000 * 60 * 60 * 24)),
    hours: Math.floor((total / (1000 * 60 * 60)) % 24),
    minutes: Math.floor((total / (1000 * 60)) % 60),
    seconds: Math.floor((total / 1000) % 60),
  };
}

export function Countdown({ target, label, onComplete, compact }: CountdownProps) {
  const [timeLeft, setTimeLeft] = useState<TimeLeft>(() => calcTimeLeft(target));

  useEffect(() => {
    const timer = setInterval(() => {
      const tl = calcTimeLeft(target);
      setTimeLeft(tl);
      if (tl.total <= 0) {
        clearInterval(timer);
        onComplete?.();
      }
    }, 1000);
    return () => clearInterval(timer);
  }, [target, onComplete]);

  if (timeLeft.total <= 0) {
    return <span className="countdown-live">Starting now!</span>;
  }

  const pad = (n: number) => String(n).padStart(2, "0");

  if (compact) {
    return (
      <span className="countdown-compact" aria-label={`${timeLeft.days}d ${timeLeft.hours}h ${timeLeft.minutes}m`}>
        {timeLeft.days > 0 && <>{timeLeft.days}d </>}
        {pad(timeLeft.hours)}:{pad(timeLeft.minutes)}:{pad(timeLeft.seconds)}
      </span>
    );
  }

  return (
    <div className="countdown" role="timer" aria-label={label ?? "Countdown"}>
      {label && <div className="countdown-label">{label}</div>}
      <div className="countdown-segments">
        {timeLeft.days > 0 && (
          <div className="countdown-segment">
            <span className="countdown-value">{timeLeft.days}</span>
            <span className="countdown-unit">days</span>
          </div>
        )}
        <div className="countdown-segment">
          <span className="countdown-value">{pad(timeLeft.hours)}</span>
          <span className="countdown-unit">hrs</span>
        </div>
        <div className="countdown-segment">
          <span className="countdown-value">{pad(timeLeft.minutes)}</span>
          <span className="countdown-unit">min</span>
        </div>
        <div className="countdown-segment">
          <span className="countdown-value">{pad(timeLeft.seconds)}</span>
          <span className="countdown-unit">sec</span>
        </div>
      </div>
    </div>
  );
}
