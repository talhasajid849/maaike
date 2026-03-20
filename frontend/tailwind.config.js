/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        // MAAIKE design tokens — identical to old CSS vars
        // Change these to retheme the entire app
        bg:      '#0d1117',
        bg2:     '#161b22',
        bg3:     '#1c2333',
        bg4:     '#21262d',
        border:  '#30363d',
        border2: '#444c56',
        text1:   '#e6edf3',
        text2:   '#8b949e',
        text3:   '#6e7681',
        teal:    '#00bfa5',
        teal2:   '#00897b',
        teal3:   '#004d40',
        blue:    '#388bfd',
        green:   '#3fb950',
        red:     '#f85149',
        yellow:  '#d29922',
        orange:  '#f0883e',
        purple:  '#bc8cff',
      },
      fontFamily: {
        sans: ['"Geist"', '"Inter"', 'system-ui', 'sans-serif'],
        mono: ['"Geist Mono"', '"Fira Code"', 'monospace'],
      },
      borderRadius: {
        sm: '6px',
        DEFAULT: '8px',
        lg: '12px',
      },
      fontSize: {
        '2xs': '10px',
        xs:    '11px',
        sm:    '12px',
        base:  '13px',
        md:    '14px',
      },
    },
  },
  plugins: [],
};