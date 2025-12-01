"use client";
import { useEffect, useState } from "react"
import './snowflake.css';
import { Button } from "@garden-co/design-system/src/components/atoms/Button";

export const AdventOfJazzBanner = () => {
  let [shouldShow, setShouldShow] = useState<boolean | null>(null);
  useEffect(() => {
    const showAdventOfJazzBanner = window.localStorage.getItem('show_advent_of_jazz_banner');
    if (showAdventOfJazzBanner === 'false' || shouldShow === false) {
      window.localStorage.setItem('show_advent_of_jazz_banner', 'false');
      document.body.style.paddingBlockEnd = '0px';
      setShouldShow(false);
      return;
    }
    document.body.style.paddingBlockEnd = '52px';
    setShouldShow(true)
  }, [shouldShow])
  if (!shouldShow) return;
  return (
    <>
      <div className="fixed bottom-0 p-2 w-full grid grid-cols-3 justify-center bg-black overflow-hidden snow gap-2 items-center"><div></div><div>🎄 ❄️ 🕯️ <a href="https://discord.gg/utDMjHYg42" className="text-white underline">Join the Advent of Jazz event on our Discord!</a> 🕯️ ❄️ 🎄</div><button className="ms-auto px-2 py-1 cursor-pointer border-2 border-white rounded-lg hover:bg-white hover:text-black text-white transition-colors self-end" onClick={() => setShouldShow(false)}>No thanks</button>
      </div >
    </>
  )
}