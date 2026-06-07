export default function PersuasionMeter({ value, turn, maxTurns }) {
  const color = value >= 80 ? 'bg-emerald-400' : value >= 50 ? 'bg-amber-400' : 'bg-rose-400'
  return (
    <div className="w-full">
      <div className="flex justify-between text-xs text-slate-400 mb-1">
        <span>설득 게이지 {value}/100</span>
        <span>턴 {turn}/{maxTurns}</span>
      </div>
      <div className="h-3 w-full rounded-full bg-slate-800 overflow-hidden">
        <div className={`meter-fill h-full rounded-full ${color}`} style={{ width: `${value}%` }} />
      </div>
    </div>
  )
}
