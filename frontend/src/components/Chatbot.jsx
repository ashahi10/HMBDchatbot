import { useState, useRef, useEffect } from 'react';
import { Box, Paper, Typography } from '@mui/material';
import ReactMarkdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import api from '../api';
import ChatInput from './ChatInput';

// Custom code block renderer that defaults to JSON highlighting.
const CodeBlock = ({ inline, className, children, ...props }) => {
  const languageMatch = /language-(\w+)/.exec(className || '');
  return !inline ? (
    <div style={{ borderRadius: '8px', padding: '1em', margin: 0 }}>
      <SyntaxHighlighter
        style={vscDarkPlus}
        language={languageMatch ? languageMatch[1] : 'json'}
        wrapLines={true}
        lineProps={{ style: { whiteSpace: 'pre-wrap', wordBreak: 'break-word' } }}
        {...props}
      >
        {String(children).replace(/\n$/, '')}
      </SyntaxHighlighter>
    </div>
  ) : (
    <code className={className} {...props}>
      {children}
    </code>
  );
};

function ChatMessages({ messages }) {
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  return (
    <Box sx={{ flex: 1, overflowY: 'auto', p: 2 }}>
      {messages.map((msg, idx) => (
        <Paper key={idx} sx={{ p: 2, mb: 2 }}>
          {msg.role === 'assistant' && msg.section && (
            <Typography variant="subtitle2">{msg.section}</Typography>
          )}
          {msg.role === 'assistant' ? (
            // Render a code block with language set to 'cypher' for Neo4j queries,
            // otherwise default to JSON highlighting.
            <ReactMarkdown components={{ code: CodeBlock }}>
              {msg.section === 'Query execution'
                ? "```cypher\n" + msg.text + "\n```"
                : "```json\n" + msg.text + "\n```"}
            </ReactMarkdown>
          ) : (
            <Typography variant="body1">{msg.content}</Typography>
          )}
        </Paper>
      ))}
      <div ref={messagesEndRef} />
    </Box>
  );
}

function Chatbot() {
  const [messages, setMessages] = useState([]);
  const [newMessage, setNewMessage] = useState('');
  const [loading, setLoading] = useState(false);

  async function submitNewMessage() {
    const trimmedMessage = newMessage.trim();
    if (!trimmedMessage || loading) return;

    setMessages(prevMessages => [...prevMessages, { role: 'user', content: trimmedMessage }]);
    setNewMessage('');
    setLoading(true);

    try {
      for await (const event of api.sendChatMessage('default-chat', trimmedMessage)) {
        if (event.text === 'DONE') continue;

        setMessages(prevMessages => {
          const updatedMessages = [...prevMessages];
          const lastMessage = updatedMessages[updatedMessages.length - 1];
          if (
            lastMessage &&
            lastMessage.role === 'assistant' &&
            lastMessage.section === event.section
          ) {
            updatedMessages[updatedMessages.length - 1] = {
              ...lastMessage,
              text: lastMessage.text + (event.text || '')
            };
          } else {
            updatedMessages.push({
              role: 'assistant',
              section: event.section,
              text: event.text || ''
            });
          }
          return updatedMessages;
        });
      }
    } catch (error) {
      console.error('Error:', error);
      setMessages(prevMessages => [
        ...prevMessages,
        { role: 'assistant', text: 'Error occurred' }
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
      <ChatMessages messages={messages} />
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
