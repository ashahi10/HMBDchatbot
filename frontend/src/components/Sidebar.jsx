import React, { useState, useEffect } from 'react';
import {
  Box,
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Divider,
  Typography,
  IconButton,
  Tooltip,
  Fab,
  ListItemIcon,
  useTheme,
  useMediaQuery,
  Chip
} from '@mui/material';
import {
  Add as AddIcon,
  Chat as ChatIcon,
  Menu as MenuIcon,
  Close as CloseIcon,
  History as HistoryIcon,
  Delete as DeleteIcon
} from '@mui/icons-material';
import api from '../api';

const DRAWER_WIDTH = 280;

function Sidebar({ 
  open, 
  onToggle, 
  currentSessionId, 
  onSelectConversation, 
  onNewChat,
  isMobile = false,
  refreshTrigger = 0  // Add refresh trigger prop
}) {
  const [conversations, setConversations] = useState([]);
  const [loading, setLoading] = useState(false);
  const theme = useTheme();

  // Load conversations when sidebar opens or refresh trigger changes
  useEffect(() => {
    loadConversations();
  }, [open, refreshTrigger]);

  // Also reload conversations when currentSessionId changes
  useEffect(() => {
    if (currentSessionId) {
      loadConversations();
    }
  }, [currentSessionId]);

  const loadConversations = async () => {
    setLoading(true);
    try {
      // For now, we'll use localStorage to track conversations
      // In a real app, this would be an API call
      const savedConversations = JSON.parse(localStorage.getItem('conversationList') || '[]');
      setConversations(savedConversations);
    } catch (error) {
      console.error('Error loading conversations:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleNewChat = () => {
    onNewChat();
    if (isMobile) {
      onToggle(); // Close sidebar on mobile after action
    }
  };

  const handleSelectConversation = (conversation) => {
    onSelectConversation(conversation);
    if (isMobile) {
      onToggle(); // Close sidebar on mobile after selection
    }
  };

  const deleteConversation = async (conversationId, event) => {
    if (event) {
      event.preventDefault();
      event.stopPropagation();
    }
    
    try {
      console.log('Deleting conversation:', conversationId);
      
      // Remove from localStorage
      const savedConversations = JSON.parse(localStorage.getItem('conversationList') || '[]');
      const initialCount = savedConversations.length;
      const updatedConversations = savedConversations.filter(conv => conv.id !== conversationId);
      
      console.log(`Conversations before delete: ${initialCount}, after: ${updatedConversations.length}`);
      
      localStorage.setItem('conversationList', JSON.stringify(updatedConversations));
      
      // Remove session data
      localStorage.removeItem(`chatHistory_${conversationId}`);
      
      // Update local state immediately
      setConversations(updatedConversations);
      
      // If we deleted the current conversation, just clear it without creating new one
      if (conversationId === currentSessionId) {
        console.log('Deleted current conversation, clearing session');
        onSelectConversation({ id: null }); // Clear current session
      }
    } catch (error) {
      console.error('Error deleting conversation:', error);
    }
  };

  const formatDate = (dateString) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffInDays = Math.floor((now - date) / (1000 * 60 * 60 * 24));
    
    if (diffInDays === 0) return 'Today';
    if (diffInDays === 1) return 'Yesterday';
    if (diffInDays < 7) return `${diffInDays} days ago`;
    return date.toLocaleDateString();
  };

  const truncateTitle = (title, maxLength = 35) => {
    if (title.length <= maxLength) return title;
    return title.substring(0, maxLength) + '...';
  };

  const drawerContent = (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Box 
        sx={{ 
          p: 2, 
          borderBottom: 1, 
          borderColor: 'divider',
          backgroundColor: 'text.primary',
          color: 'white'
        }}
      >
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Chat History
          </Typography>
          {isMobile && (
            <IconButton 
              onClick={onToggle} 
              size="small" 
              sx={{ color: 'white' }}
            >
              <CloseIcon />
            </IconButton>
          )}
        </Box>
        
        {/* New Chat Button */}
        <Box sx={{ mt: 2 }}>
          <ListItemButton
            onClick={handleNewChat}
            sx={{
              bgcolor: 'rgba(255, 255, 255, 0.1)',
              borderRadius: 2,
              '&:hover': {
                bgcolor: 'rgba(255, 255, 255, 0.2)',
              },
              px: 2,
              py: 1
            }}
          >
            <ListItemIcon sx={{ color: 'white', minWidth: 36 }}>
              <AddIcon />
            </ListItemIcon>
            <ListItemText 
              primary="New Chat" 
              primaryTypographyProps={{ 
                fontWeight: 500,
                fontSize: '0.95rem'
              }}
            />
          </ListItemButton>
        </Box>
      </Box>

      {/* Conversation List */}
      <Box sx={{ flex: 1, overflow: 'hidden' }}>
        {loading ? (
          <Box sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="body2" color="text.secondary">
              Loading conversations...
            </Typography>
          </Box>
        ) : conversations.length === 0 ? (
          <Box sx={{ p: 3, textAlign: 'center' }}>
            <HistoryIcon sx={{ fontSize: 48, color: 'text.disabled', mb: 1 }} />
            <Typography variant="body2" color="text.secondary">
              No conversations yet
            </Typography>
            <Typography variant="caption" color="text.disabled">
              Start a new chat to begin
            </Typography>
          </Box>
        ) : (
          <List sx={{ py: 1, overflow: 'auto' }}>
            {conversations.map((conversation) => (
              <ListItem 
                key={conversation.id} 
                disablePadding 
                sx={{ 
                  px: 1,
                  display: 'flex',
                  alignItems: 'center'
                }}
                secondaryAction={
                  <IconButton
                    size="small"
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      deleteConversation(conversation.id, e);
                    }}
                    sx={{
                      opacity: 0.6,
                      '&:hover': {
                        opacity: 1,
                        color: 'error.main'
                      }
                    }}
                  >
                    <DeleteIcon sx={{ fontSize: 16 }} />
                  </IconButton>
                }
              >
                <ListItemButton
                  onClick={() => handleSelectConversation(conversation)}
                  selected={conversation.id === currentSessionId}
                  sx={{
                    borderRadius: 1,
                    mb: 0.5,
                    mx: 0.5,
                    pr: 6, // Add padding to avoid overlap with delete button
                    '&.Mui-selected': {
                      bgcolor: 'grey.100',
                      '&:hover': {
                        bgcolor: 'grey.200',
                      },
                    },
                  }}
                >
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    <ChatIcon 
                      sx={{ 
                        fontSize: 18,
                        color: conversation.id === currentSessionId 
                          ? 'text.primary'
                          : 'text.secondary'
                      }} 
                    />
                  </ListItemIcon>
                  <ListItemText
                    primary={truncateTitle(conversation.title)}
                    secondary={formatDate(conversation.lastModified)}
                    primaryTypographyProps={{
                      fontSize: '0.875rem',
                      fontWeight: conversation.id === currentSessionId ? 600 : 400,
                    }}
                    secondaryTypographyProps={{
                      fontSize: '0.75rem',
                    }}
                  />
                  {conversation.messageCount && (
                    <Chip
                      label={conversation.messageCount}
                      size="small"
                      sx={{
                        height: 18,
                        fontSize: '0.7rem',
                        mr: 1,
                        bgcolor: 'background.paper',
                        color: 'text.secondary'
                      }}
                    />
                  )}
                </ListItemButton>
              </ListItem>
            ))}
          </List>
        )}
      </Box>

      {/* Footer */}
      <Box sx={{ p: 2, borderTop: 1, borderColor: 'divider' }}>
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', textAlign: 'center' }}>
          {conversations.length} conversation{conversations.length !== 1 ? 's' : ''}
        </Typography>
      </Box>
    </Box>
  );

  if (isMobile) {
    return (
      <Drawer
        anchor="left"
        open={open}
        onClose={onToggle}
        sx={{
          '& .MuiDrawer-paper': {
            width: DRAWER_WIDTH,
            boxSizing: 'border-box',
          },
        }}
      >
        {drawerContent}
      </Drawer>
    );
  }

  return (
    <Drawer
      variant="persistent"
      open={open}
      sx={{
        width: open ? DRAWER_WIDTH : 0,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: DRAWER_WIDTH,
          boxSizing: 'border-box',
          position: 'relative',
        },
        transition: theme.transitions.create('width', {
          easing: theme.transitions.easing.sharp,
          duration: theme.transitions.duration.leavingScreen,
        }),
      }}
    >
      {drawerContent}
    </Drawer>
  );
}

export default Sidebar;
