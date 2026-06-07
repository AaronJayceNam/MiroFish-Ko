// ── 수익화 스텁 (연동 지점만; 실 SDK/결제는 자리 표시) ─────────────
// 원칙: 루트박스/가챠 없음. 모든 가격 투명. 시즌패스는 코스메틱 전용(Pay-to-win 금지).
import { track } from './analytics.js'
import { grantUnlimited, addBonusRounds } from './session.js'

// 보상형 광고: 거부감 최저. 힌트/추가 라운드/재도전에 사용.
// 실연동: 광고 SDK(AdMob/Unity/IronSource 등) rewarded video 콜백을 여기에 연결.
export function watchRewardedAd(placement = 'extra_round') {
  track('ad_view', { placement })
  return new Promise((resolve) => {
    // 스텁: 1.2초 후 시청 완료로 간주. 실연동 시 onRewarded 콜백에서 resolve.
    setTimeout(() => {
      track('ad_complete', { placement })
      resolve({ completed: true })
    }, 1200)
  })
}

// IAP 상품 카탈로그 (가격 투명 표기)
export const PRODUCTS = [
  { id: 'remove_ads', name: '광고 제거', price: '₩3,300', kind: 'one_time' },
  { id: 'unlimited', name: '무제한 모드 (하루 라운드 제한 해제)', price: '₩5,500', kind: 'one_time' },
  { id: 'pack_villains', name: '시나리오 팩 · 빌런 10종', price: '₩4,400', kind: 'one_time' },
]

// 시즌패스 (코스메틱: 페르소나 스킨·뱃지). Pay-to-win 금지.
export const SEASON_PASS = {
  id: 'season_1',
  name: '시즌 1 패스 — 코스메틱 페르소나 & 뱃지',
  price: '₩9,900',
  note: '게임 난이도/승률에 영향 없음 (외형 전용)',
}

// 결제 스텁: 실연동 시 Stripe(테스트 모드) Checkout 으로 교체.
//   const r = await fetch('/api/checkout',{method:'POST',body:JSON.stringify({productId})})
//   window.location = (await r.json()).url
export async function purchase(productId) {
  track('iap_click', { productId })
  // 스텁: 실제 결제 대신 즉시 부여(데모). 프로덕션에서는 절대 클라에서 권한 부여 금지 → 서버 검증.
  await new Promise((r) => setTimeout(r, 600))
  if (productId === 'unlimited') grantUnlimited()
  if (productId === 'extra_round') addBonusRounds(1)
  track('iap_success', { productId, mode: 'stub' })
  return { ok: true, mode: 'stub' }
}
