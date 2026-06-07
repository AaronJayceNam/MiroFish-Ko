/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Pretendard', 'Apple SD Gothic Neo', 'system-ui', 'sans-serif'],
      },
      keyframes: {
        pop: { '0%': { transform: 'scale(0.96)', opacity: '0' }, '100%': { transform: 'scale(1)', opacity: '1' } },
        meter: { from: { width: '0%' } },
      },
      animation: {
        pop: 'pop 0.18s ease-out',
      },
    },
  },
  plugins: [],
}
