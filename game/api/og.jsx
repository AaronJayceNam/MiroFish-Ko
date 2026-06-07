// ── GET /api/og — 결과 공유용 OG 이미지 카드 (Edge) ───────────────
// 예: /api/og?caption=...&score=88&verdict=success&char=🧓 까칠한 할아버지&seed=abcd
import { ImageResponse } from '@vercel/og'

export const config = { runtime: 'edge' }

export default function handler(req) {
  const { searchParams } = new URL(req.url)
  const caption = (searchParams.get('caption') || '설득 AI에 도전했다').slice(0, 80)
  const score = searchParams.get('score') || '0'
  const verdict = searchParams.get('verdict') || 'fail'
  const char = (searchParams.get('char') || '🤖 미스터리 캐릭터').slice(0, 40)
  const win = verdict === 'success'

  return new ImageResponse(
    (
      <div
        style={{
          width: '1200px',
          height: '630px',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'space-between',
          padding: '64px',
          background: win
            ? 'linear-gradient(135deg,#0f172a 0%,#155e3f 100%)'
            : 'linear-gradient(135deg,#0f172a 0%,#4c1d3d 100%)',
          color: 'white',
          fontFamily: 'sans-serif',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ fontSize: 36, fontWeight: 800, opacity: 0.9 }}>설득 AI · Persuade AI</div>
          <div
            style={{
              fontSize: 30,
              padding: '8px 24px',
              borderRadius: 999,
              background: win ? '#22c55e' : '#ef4444',
              fontWeight: 800,
            }}
          >
            {win ? '설득 성공' : '설득 실패'}
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <div style={{ fontSize: 40, opacity: 0.85 }}>{char}</div>
          <div style={{ fontSize: 64, fontWeight: 900, lineHeight: 1.15 }}>“{caption}”</div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end' }}>
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            <div style={{ fontSize: 28, opacity: 0.7 }}>설득 게이지</div>
            <div style={{ fontSize: 96, fontWeight: 900, color: win ? '#4ade80' : '#fb7185' }}>{score}/100</div>
          </div>
          <div style={{ fontSize: 34, fontWeight: 800, opacity: 0.9 }}>너도 이 캐릭터 뚫어봐 →</div>
        </div>
      </div>
    ),
    { width: 1200, height: 630 },
  )
}
