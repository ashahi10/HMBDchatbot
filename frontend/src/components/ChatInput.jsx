import { Box, Paper, TextField, IconButton } from '@mui/material';
import SendIcon from '@mui/icons-material/Send';
import useAutosize from '../hooks/useAutosize';

function ChatInput({ newMessage, isLoading, setNewMessage, submitNewMessage }) {
  const textareaRef = useAutosize(newMessage);

  const handleKeyDown = (e) => {
    if (e.keyCode === 13 && !e.shiftKey && !isLoading) {
      e.preventDefault();
      submitNewMessage();
    }
  };
  
  return(
    <Box sx={{ position: 'sticky', bottom: 0, bgcolor: 'background.default', py: 2 }}>
      <Paper elevation={1} sx={{ p: 1, bgcolor: 'secondary.light', borderRadius: 3 }}>
        <Box sx={{ position: 'relative', bgcolor: 'background.paper', borderRadius: 2, border: 1, borderColor: 'primary.main' }}>
          <TextField
            fullWidth
            multiline
            variant="standard"
            placeholder="Type your message..."
            value={newMessage}
            onChange={(e) => setNewMessage(e.target.value)}
            onKeyDown={handleKeyDown}
            inputRef={textareaRef}
            InputProps={{
              disableUnderline: true,
              sx: { px: 2, py: 1.5, pr: 6, maxHeight: 120, overflow: 'auto' }
            }}
          />
          <IconButton
            color="primary"
            onClick={submitNewMessage}
            disabled={isLoading || !newMessage.trim()}
            sx={{
              position: 'absolute',
              right: 8,
              top: '50%',
              transform: 'translateY(-50%)',
              bgcolor: 'primary.main',
              color: 'white',
              '&:hover': { bgcolor: 'primary.dark' },
              '&.Mui-disabled': { bgcolor: 'action.disabledBackground', color: 'action.disabled' }
            }}
          >
            <SendIcon />
          </IconButton>
        </Box>
      </Paper>
    </Box>
  );
}

export default ChatInput;