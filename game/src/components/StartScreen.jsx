import { useMemo } from 'react'
import { scenarioFromSeed, newSeed } from '../lib/scenarios.js'
import { roundsLeft } from '../lib/session.js'

export default function StartScreen({ challengeSeed, onStart, onShowPaywall, onShowLeaderboard }) {
  // 미리보기: 챌린지 시드가 있으면 그 캐릭터, 없으면 오늘의 랜덤 미리보기
  const preview = useMemo(
    () => scenarioFromSeed(challengeSeed || 'today-' + new Date().toISOString().slice(0, 10)),
    [challengeSeed],
  )
  const left = roundsLeft()
  const canPlay = left > 0

  return (
    <div className="flex flex-col items-center text-center gap-6 px-5 pt-10">
      <div>
        <h1 className="text-3xl font-extrabold tracking-tight">설득 AI</h1>
        <p className="text-slate-400 mt-1">무작위 AI 캐릭터를 대화로 설득하라</p>
      </div>

      <div className="w-full max-w-sm rounded-3xl bg-slate-900 border border-slate-800 p-6">
        <div className="text-6xl mb-2">{preview.archetype.emoji}</div>
        <div className="text-lg font-bold">{preview.archetype.name}</div>
        <p className="text-sm text-slate-400 mt-2">{preview.archetype.persona}</p>
        <div className="mt-4 inline-block rounded-full bg-indigo-500/15 text-indigo-300 text-sm px-3 py-1">
          🎯 목표: {preview.goal.label}
        </div>
        <div className="mt-1 text-xs text-slate-500">난이도: {preview.difficulty.label}</div>
        {challengeSeed && (
          <div className="mt-3 text-xs text-emerald-400">친구의 챌린지! 같은 캐릭터에 도전합니다 🔥</div>
        )}
      </div>

      {canPlay ? (
        <button
          onClick={() => onStart(challengeSeed || newSeed())}
          className="w-full max-w-sm rounded-2xl bg-indigo-600 hover:bg-indigo-500 active:scale-[.98] transition py-4 text-lg font-bold"
        >
          {challengeSeed ? '이 캐릭터에 도전' : '오늘의 도전 시작'}
        </button>
      ) : (
        <button
          onClick={onShowPaywall}
          className="w-full max-w-sm rounded-2xl bg-amber-500 hover:bg-amber-400 text-slate-900 active:scale-[.98] transition py-4 text-lg font-bold"
        >
          오늘 무료 라운드 소진 — 더 하기
        </button>
      )}

      <div className="text-sm text-slate-500">
        오늘 남은 무료 라운드: {left === Infinity ? '무제한' : left}
      </div>

      <button onClick={onShowLeaderboard} className="text-sm text-slate-400 underline underline-offset-4">
        🏆 일일 리더보드 보기
      </button>
    </div>
  )
}
