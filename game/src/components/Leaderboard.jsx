import { useEffect, useState } from 'react'
import { getLeaderboard } from '../lib/api.js'

export default function Leaderboard({ onClose }) {
  const [data, setData] = useState({ top: [] })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getLeaderboard()
      .then(setData)
      .finally(() => setLoading(false))
  }, [])

  return (
    <div className="fixed inset-0 z-50 bg-black/70 flex items-end sm:items-center justify-center p-4">
      <div className="w-full max-w-md rounded-3xl bg-slate-900 border border-slate-800 p-5 max-h-[80vh] flex flex-col">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-bold">🏆 오늘의 리더보드</h2>
          <button onClick={onClose} className="text-slate-400 text-xl">✕</button>
        </div>
        {loading ? (
          <div className="text-slate-400 text-center py-8">불러오는 중…</div>
        ) : data.top.length === 0 ? (
          <div className="text-slate-400 text-center py-8">아직 기록이 없어요. 첫 주자가 되어보세요!</div>
        ) : (
          <ol className="space-y-1 overflow-y-auto">
            {data.top.map((r, i) => (
              <li key={i} className="flex justify-between items-center rounded-xl bg-slate-800/60 px-4 py-2.5">
                <span>
                  <span className="inline-block w-6 text-slate-400">{i + 1}</span>
                  {r.name}
                </span>
                <span className="font-bold">{r.score}/100</span>
              </li>
            ))}
          </ol>
        )}
        <p className="text-[10px] text-slate-500 mt-3 text-center">
          스텁 리더보드(휘발). 프로덕션은 Supabase로 영속화됩니다.
        </p>
      </div>
    </div>
  )
}
