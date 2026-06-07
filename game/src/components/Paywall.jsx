import { useState } from 'react'
import { PRODUCTS, SEASON_PASS, watchRewardedAd, purchase } from '../lib/monetization.js'
import { addBonusRounds } from '../lib/session.js'
import { track } from '../lib/analytics.js'

export default function Paywall({ onClose, onGranted }) {
  const [busy, setBusy] = useState(null)
  track('paywall_view')

  const onWatchAd = async () => {
    setBusy('ad')
    await watchRewardedAd('extra_round')
    addBonusRounds(1)
    setBusy(null)
    onGranted?.()
  }

  const onBuy = async (id) => {
    setBusy(id)
    await purchase(id)
    setBusy(null)
    onGranted?.()
  }

  return (
    <div className="fixed inset-0 z-50 bg-black/70 flex items-end sm:items-center justify-center p-4">
      <div className="w-full max-w-md rounded-3xl bg-slate-900 border border-slate-800 p-5">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-bold">계속 플레이하기</h2>
          <button onClick={onClose} className="text-slate-400 text-xl">✕</button>
        </div>

        {/* 거부감 최저: 광고 보고 무료 추가 */}
        <button
          onClick={onWatchAd}
          disabled={busy}
          className="w-full rounded-2xl bg-indigo-600 hover:bg-indigo-500 py-4 font-bold mb-4 disabled:opacity-50"
        >
          {busy === 'ad' ? '광고 보는 중…' : '▶ 광고 보고 +1 라운드 (무료)'}
        </button>

        <div className="text-xs text-slate-500 mb-2">또는 구매 (가격 투명 · 확률형 없음)</div>
        <div className="space-y-2">
          {PRODUCTS.map((p) => (
            <button
              key={p.id}
              onClick={() => onBuy(p.id)}
              disabled={busy}
              className="w-full flex justify-between items-center rounded-xl bg-slate-800 hover:bg-slate-700 px-4 py-3 disabled:opacity-50"
            >
              <span>{p.name}</span>
              <span className="font-bold">{busy === p.id ? '처리 중…' : p.price}</span>
            </button>
          ))}
          <button
            onClick={() => onBuy(SEASON_PASS.id)}
            disabled={busy}
            className="w-full flex justify-between items-center rounded-xl bg-gradient-to-r from-fuchsia-700/40 to-amber-600/30 hover:from-fuchsia-700/60 px-4 py-3 disabled:opacity-50"
          >
            <span className="text-left">
              {SEASON_PASS.name}
              <span className="block text-[10px] text-slate-400">{SEASON_PASS.note}</span>
            </span>
            <span className="font-bold">{SEASON_PASS.price}</span>
          </button>
        </div>

        <p className="text-[10px] text-slate-500 mt-3 text-center">
          결제는 데모(스텁)입니다. 실연동 시 Stripe 테스트 모드 + 서버 검증으로 권한이 부여됩니다.
        </p>
      </div>
    </div>
  )
}
