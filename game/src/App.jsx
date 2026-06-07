import { useEffect, useState } from 'react'
import { useGame } from './state/useGame.js'
import StartScreen from './components/StartScreen.jsx'
import GameScreen from './components/GameScreen.jsx'
import ResultCard from './components/ResultCard.jsx'
import Paywall from './components/Paywall.jsx'
import Leaderboard from './components/Leaderboard.jsx'
import { incomingChallengeSeed } from './lib/share.js'
import { trackVisit } from './lib/analytics.js'
import { canPlay } from './lib/session.js'

export default function App() {
  const game = useGame()
  const [challengeSeed, setChallengeSeed] = useState(null)
  const [showPaywall, setShowPaywall] = useState(false)
  const [showLb, setShowLb] = useState(false)
  const [, force] = useState(0)

  useEffect(() => {
    trackVisit()
    setChallengeSeed(incomingChallengeSeed())
  }, [])

  const tryStart = (seed) => {
    if (!canPlay()) {
      setShowPaywall(true)
      return
    }
    setChallengeSeed(null)
    game.startRound(seed)
  }

  return (
    <div className="h-full flex flex-col">
      <main className="flex-1 min-h-0">
        {game.phase === 'start' && (
          <StartScreen
            challengeSeed={challengeSeed}
            onStart={tryStart}
            onShowPaywall={() => setShowPaywall(true)}
            onShowLeaderboard={() => setShowLb(true)}
          />
        )}

        {game.phase === 'playing' && game.scenario && (
          <GameScreen
            scenario={game.scenario}
            guard={game.guard}
            turn={game.turn}
            maxTurns={game.maxTurns}
            messages={game.messages}
            loading={game.loading}
            error={game.error}
            onSend={game.send}
          />
        )}

        {game.phase === 'result' && game.scenario && game.result && (
          <ResultCard
            scenario={game.scenario}
            result={game.result}
            onRetry={() => tryStart()}
            onHome={game.reset}
          />
        )}
      </main>

      {showPaywall && (
        <Paywall
          onClose={() => setShowPaywall(false)}
          onGranted={() => {
            setShowPaywall(false)
            force((n) => n + 1) // 남은 라운드 표시 갱신
          }}
        />
      )}
      {showLb && <Leaderboard onClose={() => setShowLb(false)} />}
    </div>
  )
}
