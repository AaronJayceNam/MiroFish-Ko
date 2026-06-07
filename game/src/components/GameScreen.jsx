import { useEffect, useRef, useState } from 'react'
import PersuasionMeter from './PersuasionMeter.jsx'
import ChatBubble from './ChatBubble.jsx'
import { watchRewardedAd } from '../lib/monetization.js'
import { MAX_INPUT_CHARS } from '../lib/constants.js'

export default function GameScreen({ scenario, guard, turn, maxTurns, messages, loading, error, onSend }) {
  const [input, setInput] = useState('')
  const [hint, setHint] = useState(null)
  const [hintLoading, setHintLoading] = useState(false)
  const endRef = useRef(null)

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const submit = (e) => {
    e?.preventDefault()
    if (!input.trim() || loading) return
    onSend(input)
    setInput('')
  }

  // 보상형 광고 → 힌트 (캐릭터 약점 노출). 거부감 낮은 수익화.
  const getHint = async () => {
    setHintLoading(true)
    await watchRewardedAd('hint')
    setHint(scenario.archetype.guard)
    setHintLoading(false)
  }

  return (
    <div className="flex flex-col h-full max-w-lg mx-auto w-full">
      {/* 캐릭터 헤더 + 게이지 */}
      <div className="px-4 pt-3 pb-2 border-b border-slate-800 bg-slate-950/80 backdrop-blur sticky top-0 z-10">
        <div className="flex items-center gap-3 mb-2">
          <div className="text-3xl">{scenario.archetype.emoji}</div>
          <div className="min-w-0">
            <div className="font-bold truncate">{scenario.archetype.name}</div>
            <div className="text-xs text-indigo-300">🎯 {scenario.goal.label}</div>
          </div>
        </div>
        <PersuasionMeter value={guard} turn={turn} maxTurns={maxTurns} />
      </div>

      {/* 대화 영역 */}
      <div className="flex-1 overflow-y-auto px-4 py-4 space-y-3">
        <div className="text-center text-xs text-slate-500">
          {scenario.trait} · 대화로 {scenario.goal.label}를 성공시켜보세요.
        </div>
        {messages.map((m, i) => (
          <ChatBubble key={i} role={m.role} text={m.text} mood={m.mood} emoji={scenario.archetype.emoji} />
        ))}
        {loading && (
          <div className="flex justify-start">
            <div className="bg-slate-800 rounded-2xl rounded-bl-sm px-4 py-3 text-slate-400">…</div>
          </div>
        )}
        {hint && (
          <div className="text-xs text-amber-300 bg-amber-500/10 rounded-xl px-3 py-2">
            💡 힌트(약점): {hint}
          </div>
        )}
        {error && <div className="text-xs text-rose-400 text-center">{error} (다시 보내보세요)</div>}
        <div ref={endRef} />
      </div>

      {/* 입력 */}
      <form onSubmit={submit} className="p-3 border-t border-slate-800 bg-slate-950">
        <div className="flex items-center gap-2 mb-2">
          <button
            type="button"
            onClick={getHint}
            disabled={hintLoading || !!hint}
            className="text-xs rounded-full bg-slate-800 hover:bg-slate-700 px-3 py-1.5 disabled:opacity-50"
          >
            {hintLoading ? '광고 보는 중…' : hint ? '힌트 사용됨' : '💡 힌트 (광고)'}
          </button>
        </div>
        <div className="flex items-end gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value.slice(0, MAX_INPUT_CHARS))}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) submit(e)
            }}
            rows={1}
            placeholder="설득해보세요…"
            className="flex-1 resize-none rounded-2xl bg-slate-800 px-4 py-3 text-[15px] outline-none focus:ring-2 ring-indigo-500 max-h-32"
          />
          <button
            type="submit"
            disabled={loading || !input.trim()}
            className="rounded-2xl bg-indigo-600 hover:bg-indigo-500 px-5 py-3 font-bold disabled:opacity-40"
          >
            전송
          </button>
        </div>
      </form>
    </div>
  )
}
