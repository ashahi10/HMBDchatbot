import React, { useState } from 'react';
import { AppBar, Toolbar, Box, Typography, IconButton, useMediaQuery, useTheme, Fab } from '@mui/material';
import { Menu as MenuIcon, Add as AddIcon } from '@mui/icons-material';
import Chatbot from './components/Chatbot';
import Sidebar from './components/Sidebar';

function App() {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));
  const [sidebarOpen, setSidebarOpen] = useState(!isMobile);
  const [currentSessionId, setCurrentSessionId] = useState(null);
  const [refreshTrigger, setRefreshTrigger] = useState(0);

  const toggleSidebar = () => {
    setSidebarOpen(!sidebarOpen);
  };

  const handleNewChat = () => {
    // Force creation of new conversation
    setCurrentSessionId('NEW_CHAT_TRIGGER');
  };

  const handleSelectConversation = (conversation) => {
    setCurrentSessionId(conversation.id);
  };

  const handleConversationUpdate = () => {
    // Trigger sidebar refresh when conversations are updated
    setRefreshTrigger(prev => prev + 1);
  };

  return (
    <Box sx={{ display: 'flex', height: '100vh', overflow: 'hidden' }}>
      {/* Sidebar */}
      <Sidebar
        open={sidebarOpen}
        onToggle={toggleSidebar}
        currentSessionId={currentSessionId}
        onSelectConversation={handleSelectConversation}
        onNewChat={handleNewChat}
        isMobile={isMobile}
        refreshTrigger={refreshTrigger}
      />

      {/* Main content area */}
      <Box 
        sx={{ 
          flexGrow: 1, 
          display: 'flex', 
          flexDirection: 'column',
          width: isMobile ? '100%' : sidebarOpen ? 'calc(100% - 280px)' : '100%',
          transition: theme.transitions.create('width', {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.leavingScreen,
          }),
        }}
      >
        {/* App Bar */}
        <AppBar 
          position="sticky" 
          color="inherit" 
          elevation={1} 
          sx={{ 
            bgcolor: 'background.paper',
            borderBottom: 1,
            borderColor: 'divider'
          }}
        >
          <Toolbar>
            <IconButton
              edge="start"
              onClick={toggleSidebar}
              sx={{ mr: 2 }}
            >
              <MenuIcon />
            </IconButton>
            <Box component="img" src="/hmdbot.svg" alt="HMDBot" sx={{ height: 40, mr: 2 }} />
            <Typography variant="h6" color="text.primary" sx={{ flexGrow: 1 }}>
              HMDB CHATBOT
            </Typography>
            {!isMobile && (
              <IconButton
                onClick={handleNewChat}
                sx={{ 
                  bgcolor: 'text.primary',
                  color: 'white',
                  '&:hover': { bgcolor: 'grey.900' }
                }}
              >
                <AddIcon />
              </IconButton>
            )}
          </Toolbar>
        </AppBar>

        {/* Main content */}
        <Box component="main" sx={{ flexGrow: 1, overflow: 'hidden' }}>
          <Chatbot 
            selectedSessionId={currentSessionId}
            onSessionChange={setCurrentSessionId}
            onConversationUpdate={handleConversationUpdate}
          />
        </Box>

        {/* Floating Action Button for mobile */}
        {isMobile && (
          <Fab
            onClick={handleNewChat}
            sx={{
              position: 'fixed',
              bottom: 80,
              right: 16,
              zIndex: 1000,
              bgcolor: 'text.primary',
              color: 'white',
              '&:hover': { bgcolor: 'grey.900' }
            }}
          >
            <AddIcon />
          </Fab>
        )}
      </Box>
    </Box>
  );
}

export default App;
