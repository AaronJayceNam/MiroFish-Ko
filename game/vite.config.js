import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 로컬 개발 시 `vercel dev`로 /api 함수까지 함께 띄우는 것을 권장.
// 순수 `vite` 사용 시 아래 proxy로 별도 함수 서버(예: vercel dev :3000)에 연결할 수 있다.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: process.env.API_PROXY
      ? { '/api': { target: process.env.API_PROXY, changeOrigin: true } }
      : undefined,
  },
})
