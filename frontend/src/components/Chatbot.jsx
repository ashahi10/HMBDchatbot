import { useState, useRef, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  CircularProgress,
  Divider
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ReactMarkdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';

import api from '../api';
import ChatInput from './ChatInput';

//
// CodeBlock - a custom renderer for fenced code blocks in ReactMarkdown.
//
const CodeBlock = ({ inline, className, children, ...props }) => {
  const languageMatch = /language-(\w+)/.exec(className || '');
  
  if (inline) {
    return (
      <code className={className} {...props}>
        {children}
      </code>
    );
  }

  return (
    <Box sx={{ borderRadius: 1, overflow: 'hidden', my: 1 }}>
      <SyntaxHighlighter
        style={vscDarkPlus}
        language={languageMatch ? languageMatch[1] : 'json'}
        wrapLines
        lineProps={{ style: { whiteSpace: 'pre-wrap', wordBreak: 'break-word' } }}
        {...props}
      >
        {String(children).replace(/\n$/, '')}
      </SyntaxHighlighter>
    </Box>
  );
};

//
// ChatMessages
// - Groups messages into "chunks", each chunk representing:
//    1) One user prompt
//    2) Assistant's "reasoning" (non-summary) content
//    3) Assistant's summary (if any)
// - Renders each chunk as two Papers:
//    a) One for the user query
//    b) One for the assistant's entire response
//
function ChatMessages({ messages, loading }) {
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Build an array of "chunks" each time we see a user message:
  // {
  //   userText: string,
  //   nonSummaryItems: [{ section, codeBlock }...],
  //   summaryText: string
  // }
  const chunks = [];
  let currentChunk = null;

  messages.forEach((msg) => {
    if (msg.role === 'user') {
      // Start a new chunk for each user prompt
      if (currentChunk) {
        chunks.push(currentChunk);
      }
      currentChunk = {
        userText: msg.content,
        nonSummaryItems: [],
        summaryText: ''
      };
    } else if (msg.role === 'assistant') {
      // If assistant messages arrive before user, create a chunk on-the-fly
      if (!currentChunk) {
        currentChunk = {
          userText: '',
          nonSummaryItems: [],
          summaryText: ''
        };
      }

      // If it's a Summary, append to summaryText; otherwise, it's "reasoning"
      if (msg.section === 'Summary') {
        currentChunk.summaryText += msg.text;
      } else {
        // Example: "Query execution" -> ```cypher\n...\n```, else ```json\n...\n```
        const codeBlock =
          msg.section === 'Query execution'
            ? `\`\`\`cypher\n${msg.text}\n\`\`\``
            : `\`\`\`json\n${msg.text}\n\`\`\``;

        currentChunk.nonSummaryItems.push({
          section: msg.section,
          codeBlock
        });
      }
    }
  });

  if (currentChunk) {
    chunks.push(currentChunk);
  }

  return (
    <Box sx={{ flex: 1, overflowY: 'auto', p: 2 }}>
      {chunks.map((chunk, idx) => {
        const hasNonSummary = chunk.nonSummaryItems.length > 0;
        const hasSummary = chunk.summaryText && chunk.summaryText.trim().length > 0;

        return (
          <Box key={idx} sx={{ mb: 4 }}>
            {/* (1) User Prompt */}
            <Paper
              variant="outlined"
              sx={{
                p: 2,
                mb: 1,
                borderRadius: 2,
                borderColor: 'divider',
                bgcolor: '#f9f9f9'
              }}
            >
              <Typography variant="subtitle2" gutterBottom>
                You asked:
              </Typography>
              <Typography variant="body1">{chunk.userText}</Typography>
            </Paper>

            {/* (2) Assistant's Response in a single Paper */}
            <Paper
              variant="outlined"
              sx={{
                p: 2,
                borderRadius: 2,
                borderColor: 'divider'
              }}
            >
              {/* If there's non-Summary "reasoning", show it in a subtle Accordion */}
              {hasNonSummary && (
                <>
                  <Accordion
                    sx={{
                      mb: 2,
                      boxShadow: 'none',
                      border: '1px solid #ddd',
                      backgroundColor: '#fafafa',
                      '&:before': { display: 'none' } // removes default MUI divider line
                    }}
                    disableGutters
                  >
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Box sx={{ display: 'flex', alignItems: 'center' }}>
                        <Typography variant="body2" fontWeight={500}>
                          Reasoning
                        </Typography>
                        {loading && !hasSummary && (
                          <CircularProgress size={16} sx={{ ml: 2 }} />
                        )}
                      </Box>
                    </AccordionSummary>
                    <AccordionDetails>
                      {chunk.nonSummaryItems.map((item, i) => (
                        <Box key={i} sx={{ mb: 3 }}>
                          {item.section && (
                            <Typography variant="subtitle2" sx={{ mb: 1 }}>
                              {item.section}
                            </Typography>
                          )}
                          <ReactMarkdown components={{ code: CodeBlock }}>
                            {item.codeBlock}
                          </ReactMarkdown>
                          {i < chunk.nonSummaryItems.length - 1 && (
                            <Divider sx={{ my: 2 }} />
                          )}
                        </Box>
                      ))}
                    </AccordionDetails>
                  </Accordion>
                  {/* Add a small divider between "reasoning" and the summary (if it exists) */}
                  {hasSummary && <Divider sx={{ mb: 2 }} />}
                </>
              )}

              {/* Show the summary below the 'reasoning' if it exists */}
              {hasSummary && (
                <>
                  <Typography variant="subtitle1" fontWeight={500} gutterBottom>
                    Answer
                  </Typography>
                  <Typography variant="body1">
                    {chunk.summaryText}
                  </Typography>
                </>
              )}

              {/* If there's no nonSummary and no summary, that means assistant gave no response yet */}
              {!hasNonSummary && !hasSummary && loading && (
                <Box sx={{ display: 'flex', alignItems: 'center', mt: 1 }}>
                  <CircularProgress size={16} sx={{ mr: 1 }} />
                  <Typography variant="body2" color="text.secondary">
                    Waiting for assistant...
                  </Typography>
                </Box>
              )}
            </Paper>
          </Box>
        );
      })}
      <div ref={messagesEndRef} />
    </Box>
  );
}

//
// Chatbot (the main container)
//
function Chatbot() {
  const [messages, setMessages] = useState([]);
  const [newMessage, setNewMessage] = useState('');
  const [loading, setLoading] = useState(false);

  async function submitNewMessage() {
    const trimmedMessage = newMessage.trim();
    if (!trimmedMessage || loading) return;

    setMessages((prev) => [...prev, { role: 'user', content: trimmedMessage }]);
    setNewMessage('');
    setLoading(true);

    try {
      for await (const event of api.sendChatMessage('default-chat', trimmedMessage)) {
        if (event.text === 'DONE') continue;

        setMessages((prev) => {
          const updated = [...prev];
          const last = updated[updated.length - 1];

          if (last && last.role === 'assistant' && last.section === event.section) {
            // Append text to the last assistant message if same 'section'
            updated[updated.length - 1] = {
              ...last,
              text: (last.text || '') + (event.text || '')
            };
          } else {
            // Otherwise, create a new assistant message
            updated.push({
              role: 'assistant',
              section: event.section,
              text: event.text || ''
            });
          }
          return updated;
        });
      }
    } catch (err) {
      console.error('Error:', err);
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', text: 'Error occurred' }
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
      {/* Pass messages and loading state to ChatMessages */}
      <ChatMessages messages={messages} loading={loading} />
      <ChatInput
        newMessage={newMessage}
        isLoading={loading}
        setNewMessage={setNewMessage}
        submitNewMessage={submitNewMessage}
      />
    </Box>
  );
}

export default Chatbot;
