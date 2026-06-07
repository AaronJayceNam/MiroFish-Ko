import { useEffect, useState } from 'react'
import { shareResult, ogImageUrl } from '../lib/share.js'
import { submitScore } from '../lib/api.js'
import { sessionId } from '../lib/session.js'

export default function ResultCard({ scenario, result, onRetry, onHome }) {
  const win = result.verdict === 'success'
  const char = `${scenario.archetype.emoji} ${scenario.archetype.name}`
  const [shared, setShared] = useState(false)

  // 결과 자동 리더보드 제출
  useEffect(() => {
    submitScore({ name: '익명' + sessionId().slice(-4), score: result.score, seed: scenario.seed })
  }, [])

  const og = ogImageUrl({ caption: result.caption, score: result.score, verdict: result.verdict, char })

  const onShare = async () => {
    const r = await shareResult({
      seed: scenario.seed,
      caption: result.caption,
      score: result.score,
      verdict: result.verdict,
      char,
    })
    if (r.shared) setShared(true)
  }

  return (
    <div className="flex flex-col items-center text-center gap-5 px-5 pt-8 max-w-md mx-auto w-full">
      <div className={`text-2xl font-extrabold ${win ? 'text-emerald-400' : 'text-rose-400'}`}>
        {win ? '🎉 설득 성공!' : '💢 설득 실패'}
      </div>

      {/* 공유 카드 미리보기 (OG와 동일 데이터) */}
      <div className="w-full rounded-3xl overflow-hidden border border-slate-800 bg-slate-900">
        <img src={og} alt="결과 카드" className="w-full" />
      </div>

      <div className="text-sm text-slate-300 bg-slate-800/60 rounded-2xl px-4 py-3">
        <div className="font-semibold mb-1">판정 이유</div>
        {result.reason}
      </div>

      <button
        onClick={onShare}
        className="w-full rounded-2xl bg-emerald-600 hover:bg-emerald-500 active:scale-[.98] transition py-4 text-lg font-bold"
      >
        {shared ? '공유 완료! (+1 보너스 라운드)' : '📤 결과 공유하고 보너스 받기'}
      </button>
      <p className="text-xs text-slate-500 -mt-2">친구가 같은 캐릭터에 도전해요 · 공유 시 보너스 라운드 +1</p>

      <div className="flex gap-3 w-full">
        <button onClick={onRetry} className="flex-1 rounded-2xl bg-slate-800 hover:bg-slate-700 py-3 font-bold">
          🔁 재도전
        </button>
        <button onClick={onHome} className="flex-1 rounded-2xl bg-slate-800 hover:bg-slate-700 py-3 font-bold">
          🏠 홈
        </button>
      </div>
    </div>
  )
}
