// ── Anthropic 호출 래퍼 (서버 전용) ──────────────────────────────
// 공식 SDK 사용. API 키는 서버 환경변수에서만 읽는다(클라 노출 금지).
import Anthropic from '@anthropic-ai/sdk'
import { SYSTEM_RULES, RESPONSE_SCHEMA, characterBlock, playerTurn } from './prompt.js'

const MODEL = process.env.MODEL || 'claude-opus-4-8'

let _client
function client() {
  if (!_client) _client = new Anthropic() // ANTHROPIC_API_KEY 환경변수 자동 사용
  return _client
}

function transcript(scenario, history) {
  if (!history?.length) return '[지금까지의 대화 없음 — 첫 턴]'
  const name = scenario.archetype.name
  const lines = history.map((h) =>
    h.role === 'player' ? `플레이어: ${h.text}` : `${name}: ${h.text}`,
  )
  return '[지금까지의 대화]\n' + lines.join('\n')
}

// 한 턴 판정. 구조화 JSON을 강제로 받아 파싱해 반환.
export async function judgeTurn({ scenario, history, message, guard, turn, maxTurns }) {
  const userContent =
    characterBlock(scenario, guard, turn, maxTurns) +
    '\n\n' +
    transcript(scenario, history) +
    '\n\n' +
    playerTurn(message)

  const res = await client().messages.create({
    model: MODEL,
    max_tokens: 500,
    // 게임 NPC는 저지연이 중요 → 사고 비활성 + 낮은 effort로 비용/속도 최적.
    thinking: { type: 'disabled' },
    system: [
      { type: 'text', text: SYSTEM_RULES, cache_control: { type: 'ephemeral' } },
    ],
    output_config: {
      effort: 'low',
      format: { type: 'json_schema', schema: RESPONSE_SCHEMA },
    },
    messages: [{ role: 'user', content: userContent }],
  })

  if (res.stop_reason === 'refusal') {
    // 안전 거절 → 게임상 캐릭터가 응하지 않은 것으로 처리
    return {
      npc_reply: '...그건 좀 곤란한데요.',
      mood: 'offended',
      delta: 0,
      persuasion: guard,
      goal_met: false,
      verdict: turn >= maxTurns ? 'fail' : 'in_progress',
      reason: '부적절한 시도로 캐릭터가 응하지 않았습니다.',
      clip_caption: '선 넘었다가 거절당함 🚫',
    }
  }

  const text = res.content.find((b) => b.type === 'text')?.text || '{}'
  let parsed
  try {
    parsed = JSON.parse(text)
  } catch {
    parsed = {}
  }
  return { ...parsed, _usage: res.usage }
}

export { MODEL }
