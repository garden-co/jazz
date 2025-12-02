"use client";
import { useEffect, useState } from "react"
import './snowflake.css';

export const AdventOfJazzBanner = () => {
  let [shouldShow, setShouldShow] = useState<boolean | null>(null);
  useEffect(() => {
    // Check if we're past the event date (client-side only to avoid hydration mismatch)
    if (new Date() >= new Date("2026-01-01")) {
      setShouldShow(false);
      return;
    }

    const showAdventOfJazzBanner = window.localStorage.getItem('show_advent_of_jazz_banner');
    if (showAdventOfJazzBanner === 'false') {
      setShouldShow(false);
      return;
    }
    document.body.style.paddingBlockEnd = '52px';
    setShouldShow(true);

    return () => {
      document.body.style.paddingBlockEnd = '0px';
    };
  }, [])

  const handleDismiss = () => {
    window.localStorage.setItem('show_advent_of_jazz_banner', 'false');
    document.body.style.paddingBlockEnd = '0px';
    setShouldShow(false);
  };

  if (!shouldShow) return;
  return (
    <>
      <div className="fixed bottom-0 p-4 w-full flex items-center justify-start md:justify-center bg-black overflow-hidden snow gap-2 items-center text-center z-99">

        <div className="flex md:block gap-2 items-center ms-auto">
          <div className="md:contents">🎄<span className="hidden md:contents"> ❄️ 🕯️</span></div>
          <a href="https://discord.gg/utDMjHYg42" className="text-white underline">Join the Advent of Jazz event on our Discord!</a>
          <div className="md:contents"><span className="hidden  md:contents">🕯️ ❄️</span> 🎄</div>
        </div>
        <button className="ms-auto px-2 py-1 cursor-pointer border-2 border-white rounded-lg hover:bg-white hover:text-black text-white transition-colors self-end whitespace-nowrap" onClick={handleDismiss}>No thanks</button>
      </div>
    </>
  )
}