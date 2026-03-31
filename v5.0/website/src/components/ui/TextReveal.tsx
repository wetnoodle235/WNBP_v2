"use client";

import { useState, useEffect, useRef, useCallback } from "react";

interface TextRevealProps {
  /** Array of strings to cycle through */
  phrases: string[];
  /** Time each character takes to type (ms) */
  typingSpeed?: number;
  /** Time each character takes to delete (ms) */
  deletingSpeed?: number;
  /** Pause before starting to delete (ms) */
  pauseDuration?: number;
  /** CSS class for the wrapper */
  className?: string;
  /** Tag to render as */
  as?: "span" | "h1" | "h2" | "h3" | "p";
}

/**
 * Typewriter-style text animation that cycles through phrases.
 * Respects prefers-reduced-motion — static text shown when motion is reduced.
 */
export function TextReveal({
  phrases,
  typingSpeed = 80,
  deletingSpeed = 40,
  pauseDuration = 2000,
  className,
  as: Tag = "span",
}: TextRevealProps) {
  const [text, setText] = useState("");
  const [phraseIdx, setPhraseIdx] = useState(0);
  const [isDeleting, setIsDeleting] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const reducedMotion = useRef(false);

  useEffect(() => {
    reducedMotion.current = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  }, []);

  const tick = useCallback(() => {
    if (reducedMotion.current) {
      setText(phrases[phraseIdx]);
      return;
    }
    const current = phrases[phraseIdx];

    if (!isDeleting) {
      setText(current.slice(0, text.length + 1));
      if (text.length + 1 === current.length) {
        timerRef.current = setTimeout(() => setIsDeleting(true), pauseDuration);
        return;
      }
    } else {
      setText(current.slice(0, text.length - 1));
      if (text.length - 1 === 0) {
        setIsDeleting(false);
        setPhraseIdx((prev) => (prev + 1) % phrases.length);
      }
    }
  }, [text, isDeleting, phraseIdx, phrases, pauseDuration]);

  useEffect(() => {
    if (reducedMotion.current) {
      setText(phrases[phraseIdx]);
      return;
    }
    timerRef.current = setTimeout(tick, isDeleting ? deletingSpeed : typingSpeed);
    return () => clearTimeout(timerRef.current);
  }, [tick, isDeleting, deletingSpeed, typingSpeed, phrases, phraseIdx]);

  return (
    <Tag className={className} aria-label={phrases[phraseIdx]}>
      {text}
      <span className="text-reveal-cursor" aria-hidden="true">|</span>
    </Tag>
  );
}
