const MOOD_EMOJI = {
  amused: '😏',
  annoyed: '😤',
  suspicious: '🤨',
  warming: '🙂',
  won_over: '😊',
  offended: '😠',
}

export default function ChatBubble({ role, text, mood, emoji }) {
  const isPlayer = role === 'player'
  return (
    <div className={`flex ${isPlayer ? 'justify-end' : 'justify-start'} animate-pop`}>
      <div
        className={`max-w-[80%] rounded-2xl px-4 py-2.5 text-[15px] leading-relaxed ${
          isPlayer
            ? 'bg-indigo-600 text-white rounded-br-sm'
            : 'bg-slate-800 text-slate-100 rounded-bl-sm'
        }`}
      >
        {!isPlayer && (
          <span className="mr-1">
            {emoji} {mood ? MOOD_EMOJI[mood] || '' : ''}
          </span>
        )}
        {text}
      </div>
    </div>
  )
}
