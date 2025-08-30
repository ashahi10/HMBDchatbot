import React from 'react'
import ReactDOM from 'react-dom/client';
import { CssBaseline, ThemeProvider, createTheme } from '@mui/material'
import App from './App.jsx'

const theme = createTheme({
  palette: {
    primary: { main: '#111827', light: '#e5e7eb', dark: '#0b1220', contrastText: '#ffffff' },
    secondary: { main: '#f3f4f6', light: '#f8fafc', dark: '#e5e7eb' },
    background: { default: '#ffffff', paper: '#ffffff' }
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    h1: { fontSize: '1.5rem', fontWeight: 700 },
    subtitle1: { fontSize: '0.875rem' }
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: { height: '100vh' },
        '#root': { height: '100%' }
      }
    }
  }
});

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <App />
    </ThemeProvider>
  </React.StrictMode>,
);