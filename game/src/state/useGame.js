// ── 게임 상태 훅: 코어 루프 (진입→대화→판정→결과) ────────────────
import { useCallback, useState } from 'react'
import { scenarioFromSeed, newSeed } from '../lib/scenarios.js'
import { persuade } from '../lib/api.js'
import { track } from '../lib/analytics.js'
import { consumeRound } from '../lib/session.js'

export function useGame() {
  const [phase, setPhase] = useState('start') // start | playing | result
  const [scenario, setScenario] = useState(null)
  const [guard, setGuard] = useState(0)
  const [turn, setTurn] = useState(1)
  const [maxTurns, setMaxTurns] = useState(6)
  const [messages, setMessages] = useState([]) // {role:'player'|'npc', text, mood}
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [result, setResult] = useState(null) // {verdict, score, caption, reason}

  const startRound = useCallback((seed) => {
    const s = seed || newSeed()
    const sc = scenarioFromSeed(s)
    setScenario(sc)
    setGuard(sc.difficulty.startGuard)
    setTurn(1)
    setMessages([])
    setResult(null)
    setError(null)
    setPhase('playing')
    consumeRound()
    track('round_start', { seed: s, archetype: sc.archetype.id, goal: sc.goal.id, difficulty: sc.difficulty.id })
  }, [])

  const send = useCallback(
    async (text) => {
      if (!scenario || loading) return
      const trimmed = text.trim()
      if (!trimmed) return
      setError(null)
      setLoading(true)
      const history = messages.map((m) => ({ role: m.role, text: m.text }))
      setMessages((prev) => [...prev, { role: 'player', text: trimmed }])

      try {
        const r = await persuade({ seed: scenario.seed, message: trimmed, guard, turn, history })
        setGuard(r.guard)
        setMessages((prev) => [...prev, { role: 'npc', text: r.npc_reply, mood: r.mood }])

        if (r.verdict !== 'in_progress') {
          const res = {
            verdict: r.verdict,
            score: r.guard,
            caption: r.clip_caption,
            reason: r.reason,
          }
          setResult(res)
          setPhase('result')
          track('round_complete', { verdict: r.verdict, score: r.guard, turns: turn })
        } else {
          setTurn((t) => t + 1)
        }
      } catch (e) {
        setError(e.message || '오류가 발생했어요.')
        // 실패 시 방금 추가한 플레이어 메시지 유지(재시도 가능)
      } finally {
        setLoading(false)
      }
    },
    [scenario, loading, messages, guard, turn],
  )

  const reset = useCallback(() => {
    setPhase('start')
    setScenario(null)
    setResult(null)
    setMessages([])
  }, [])

  return { phase, scenario, guard, turn, maxTurns, messages, loading, error, result, startRound, send, reset, setMaxTurns }
}
