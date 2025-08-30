import { Box, Paper, TextField, IconButton, Tooltip, ToggleButton } from '@mui/material';
import SendIcon from '@mui/icons-material/Send';
import ScienceIcon from '@mui/icons-material/Science';
import useAutosize from '../hooks/useAutosize';

function ChatInput({ newMessage, isLoading, setNewMessage, submitNewMessage, spectrumMode, setSpectrumMode }) {
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
        {/* Spectrum Mode Toggle */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1, px: 1 }}>
          <Tooltip title={spectrumMode ? "Spectrum Mode ON - questions will prioritize spectrum analysis" : "Spectrum Mode OFF - questions use general analysis"} arrow>
            <ToggleButton
              value="spectrum"
              selected={spectrumMode}
              onChange={() => setSpectrumMode(!spectrumMode)}
              size="small"
              sx={{
                border: 1,
                borderColor: spectrumMode ? 'success.main' : 'grey.400',
                bgcolor: spectrumMode ? 'success.light' : 'background.paper',
                color: spectrumMode ? 'success.dark' : 'text.secondary',
                '&:hover': { 
                  bgcolor: spectrumMode ? 'success.main' : 'grey.100',
                  borderColor: spectrumMode ? 'success.dark' : 'grey.600'
                },
                '&.Mui-selected': {
                  bgcolor: 'success.light',
                  color: 'success.dark',
                  '&:hover': { bgcolor: 'success.main' }
                }
              }}
            >
              <ScienceIcon sx={{ mr: 0.5, fontSize: 16 }} />
              {spectrumMode ? 'Spectrum Mode ON' : 'Spectrum Mode'}
            </ToggleButton>
          </Tooltip>
          
          {spectrumMode && (
            <Box sx={{ fontSize: '0.75rem', color: 'success.dark', fontWeight: 500 }}>
              🔬 Spectrum analysis enabled
            </Box>
          )}
        </Box>
        
        <Box sx={{ position: 'relative', bgcolor: 'background.paper', borderRadius: 2, border: 1, borderColor: 'text.primary' }}>
          <TextField
            fullWidth
            multiline
            variant="standard"
            placeholder={spectrumMode ? "Ask spectrum-related questions..." : "Type your message..."}
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
            onClick={submitNewMessage}
            disabled={isLoading || !newMessage.trim()}
            sx={{
              position: 'absolute',
              right: 8,
              top: '50%',
              transform: 'translateY(-50%)',
              bgcolor: 'text.primary',
              color: 'white',
              '&:hover': { bgcolor: 'grey.900' },
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