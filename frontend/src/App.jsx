import React from 'react';
import { AppBar, Toolbar, Box, Typography, Container } from '@mui/material';
import Chatbot from './components/Chatbot';

function App() {
  return (
    <Container maxWidth="md" sx={{ display: 'flex', flexDirection: 'column', height: '100%', px: { xs: 2, md: 3 }, py: { xs: 2, md: 4 } }}>
      <AppBar position="sticky" color="inherit" elevation={0} sx={{ mb: 2 }}>
        <Toolbar>
          <Box component="img" src="/hmdbot.svg" alt="HMDBot" sx={{ height: 100, mr: 2 }} />
          <Typography variant="h6" color="primary">Human Metabolite Database Chatbot</Typography>
        </Toolbar>
      </AppBar>

      <Box component="main" sx={{ flexGrow: 1 }}>
        <Chatbot />
      </Box>
    </Container>
  );
}

export default App;
