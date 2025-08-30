import { useState, useRef, useEffect } from 'react';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';
import 'katex/dist/katex.min.css';

import {
  Box,
  Paper,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  CircularProgress,
  Divider,
  Button,
  Chip
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ScienceIcon from '@mui/icons-material/Science';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import ReactMarkdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';

import api from '../api';
import ChatInput from './ChatInput';

//
// PHASE 2 - Transform summary text to convert pathway URLs into markdown format
//
function transformSummary(summaryText) {
  if (!summaryText || (!summaryText.includes('smpdb.ca') && !summaryText.includes('kegg.jp'))) {
    return summaryText;
  }

  let transformedText = summaryText;

  // Transform SMPDB URLs - Enhanced to handle **** and other characters after URLs
  // Pattern: "pathway name SMPDB: URL" followed by optional **** or other characters
  const smpdbPattern = /(?:^|\n)(\d+\.\s*)?([^.\n]+?)\s+SMPDB:\s+(https:\/\/smpdb\.ca\/view\/\S+)[\*\s]*(?:[\*\s]*View in KEGG.*?)?(?=\n|$)/gim;
  transformedText = transformedText.replace(smpdbPattern, (match, numbering, pathwayName, url) => {
    const cleanName = pathwayName.trim()
      .replace(/^\d+\.\s*/, '') // Remove numbering if present
      .replace(/\s+/g, ' ') // Normalize whitespace
      .replace(/\*+/g, ''); // Remove asterisks
    return `${numbering || ''}**${cleanName}** [🔬 SMPDB](${url})\n`;
  });

  // Transform KEGG URLs - Enhanced to handle **** and other characters after URLs
  // Pattern: "pathway name KEGG: URL" followed by optional **** or other characters
  const keggPattern = /(?:^|\n)(\d+\.\s*)?([^.\n]+?)\s+KEGG:\s+(https:\/\/(?:www\.)?kegg\.jp\/pathway\/\S+)[\*\s]*(?=\n|$)/gim;
  transformedText = transformedText.replace(keggPattern, (match, numbering, pathwayName, url) => {
    const cleanName = pathwayName.trim()
      .replace(/^\d+\.\s*/, '') // Remove numbering if present
      .replace(/\s+/g, ' ') // Normalize whitespace
      .replace(/\*+/g, ''); // Remove asterisks
    return `${numbering || ''}**${cleanName}** [🧬 KEGG](${url})\n`;
  });

  // Handle existing markdown-formatted links that might have been missed
  transformedText = transformedText.replace(/\[View in SMPDB\]\((https:\/\/smpdb\.ca\/view\/\S+)\)/gi, '[🔬 SMPDB]($1)');
  transformedText = transformedText.replace(/\[View in KEGG\]\((https:\/\/(?:www\.)?kegg\.jp\/pathway\/\S+)\)/gi, '[🧬 KEGG]($1)');

  // Multi-step cleanup: Remove leftover asterisks and "View in KEGG" text
  // Remove any remaining "View in KEGG" text that wasn't caught by the main patterns
  transformedText = transformedText.replace(/View in KEGG[^\n]*?(?=\n|$)/gi, '');
  
  // Remove only the specific problematic pattern: 4 consecutive asterisks
  transformedText = transformedText.replace(/\*{4}/g, '');
  
  // Clean up any extra whitespace that might have been left behind
  transformedText = transformedText.replace(/\s+$/gm, ''); // Remove trailing whitespace from lines
  transformedText = transformedText.replace(/\n{3,}/g, '\n\n'); // Replace multiple newlines with double newlines

  return transformedText;
}

//
// PHASE 3 - Pathway Card Component for inline styling
//
const PathwayCard = ({ pathwayName, url, type }) => {
  const handleClick = () => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  const getTypeConfig = (type) => {
    switch (type) {
      case 'SMPDB':
        return {
          emoji: '🔬',
          label: 'SMPDB',
          gradient: 'linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%)',
          borderColor: '#2196f3',
          textColor: '#1565c0'
        };
      case 'KEGG':
        return {
          emoji: '🧬',
          label: 'KEGG',
          gradient: 'linear-gradient(135deg, #e8f5e8 0%, #c8e6c9 100%)',
          borderColor: '#4caf50',
          textColor: '#2e7d32'
        };
      default:
        return {
          emoji: '🌐',
          label: 'Pathway',
          gradient: 'linear-gradient(135deg, #f5f5f5 0%, #eeeeee 100%)',
          borderColor: '#9e9e9e',
          textColor: '#616161'
        };
    }
  };

  const config = getTypeConfig(type);

  return (
    <Box
      sx={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 1,
        ml: 1,
        mb: 0.5
      }}
    >
      <Button
        size="small"
        variant="outlined"
        onClick={handleClick}
        sx={{
          minWidth: 'auto',
          px: 1.5,
          py: 0.5,
          fontSize: '0.75rem',
          fontWeight: 500,
          textTransform: 'none',
          borderRadius: 3,
          border: `1px solid ${config.borderColor}`,
          background: config.gradient,
          color: config.textColor,
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
          '&:hover': {
            borderColor: config.borderColor,
            background: config.gradient,
            filter: 'brightness(0.95)',
            boxShadow: '0 2px 6px rgba(0,0,0,0.15)',
            transform: 'translateY(-1px)'
          },
          transition: 'all 0.2s ease-in-out'
        }}
      >
        {config.emoji} {config.label}
      </Button>
    </Box>
  );
};

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
// Helper function to extract pathway name from URL if needed
//
const extractPathwayNameFromUrl = (url, type) => {
  try {
    if (type === 'SMPDB' && url.includes('smpdb.ca/view/')) {
      // Extract SMP ID and create a readable name
      const smpId = url.split('/').pop();
      return `${smpId} Pathway`;
    }
    if (type === 'KEGG' && url.includes('kegg.jp/pathway/')) {
      // Extract pathway ID and create a readable name
      const pathwayId = url.split('/').pop();
      return `${pathwayId} Pathway`;
    }
  } catch (error) {
    console.warn('Error extracting pathway name from URL:', error);
  }
  return `${type} Pathway`;
};

//
// PHASE 1 - Enhanced Link Component for ReactMarkdown
//
const CustomLinkComponent = ({ node, children, href, ...props }) => {
  // Debug logging to see what's happening
  console.log('CustomLinkComponent called with:', { href, children, node });

  // Detect SMPDB URLs
  if (href && href.includes('smpdb.ca/view/')) {
    // Extract pathway name from children (markdown link text)
    const linkText = typeof children[0] === 'string' ? children[0] : '';
    console.log('SMPDB link detected, linkText:', linkText);
    
    // Check if it's in the new emoji format "🔬 SMPDB"
    if (linkText.includes('🔬') || linkText.includes('SMPDB')) {
      let pathwayName = 'SMPDB Pathway';
      
      // Try multiple approaches to extract pathway name
      // Approach 1: Check the parent node structure
      if (node && node.parent && node.parent.children) {
        const linkIndex = node.parent.children.findIndex(child => child === node);
        console.log('Link index in parent:', linkIndex, 'Total children:', node.parent.children.length);
        
        // Look for strong (bold) text before this link
        for (let i = linkIndex - 1; i >= 0; i--) {
          const prevNode = node.parent.children[i];
          console.log('Checking previous node:', prevNode);
          
          if (prevNode && prevNode.type === 'strong' && prevNode.children && prevNode.children[0]) {
            pathwayName = prevNode.children[0].value || pathwayName;
            console.log('Found pathway name from strong node:', pathwayName);
            break;
          }
          
          // Also check for text nodes that might contain the pathway name
          if (prevNode && prevNode.type === 'text' && prevNode.value) {
            // Look for text that ends with "**" (indicating it might be before a bold section)
            const textValue = prevNode.value.trim();
            if (textValue.length > 0 && !textValue.includes('http')) {
              pathwayName = textValue.replace(/\*+$/, '').trim();
              console.log('Found pathway name from text node:', pathwayName);
              break;
            }
          }
        }
      }
      
      // Approach 2: Check parent's parent (paragraph level)
      if (pathwayName === 'SMPDB Pathway' && node && node.parent && node.parent.parent && node.parent.parent.children) {
        console.log('Trying paragraph level extraction');
        for (const child of node.parent.parent.children) {
          if (child.type === 'strong' && child.children && child.children[0] && child.children[0].value) {
            pathwayName = child.children[0].value;
            console.log('Found pathway name from paragraph level:', pathwayName);
            break;
          }
        }
      }
      
      // Fallback to URL extraction if no context found
      if (pathwayName === 'SMPDB Pathway') {
        pathwayName = extractPathwayNameFromUrl(href, 'SMPDB');
        console.log('Using fallback pathway name:', pathwayName);
      }
      
      console.log('Rendering SMPDB PathwayCard with name:', pathwayName);
      return <PathwayCard pathwayName={pathwayName} url={href} type="SMPDB" />;
    }
  }

  // Detect KEGG URLs
  if (href && href.includes('kegg.jp/pathway/')) {
    // Extract pathway name from children (markdown link text)
    const linkText = typeof children[0] === 'string' ? children[0] : '';
    console.log('KEGG link detected, linkText:', linkText);
    
    // Check if it's in the new emoji format "🧬 KEGG"
    if (linkText.includes('🧬') || linkText.includes('KEGG')) {
      let pathwayName = 'KEGG Pathway';
      
      // Try multiple approaches to extract pathway name
      // Approach 1: Check the parent node structure
      if (node && node.parent && node.parent.children) {
        const linkIndex = node.parent.children.findIndex(child => child === node);
        console.log('Link index in parent:', linkIndex, 'Total children:', node.parent.children.length);
        
        // Look for strong (bold) text before this link
        for (let i = linkIndex - 1; i >= 0; i--) {
          const prevNode = node.parent.children[i];
          console.log('Checking previous node:', prevNode);
          
          if (prevNode && prevNode.type === 'strong' && prevNode.children && prevNode.children[0]) {
            pathwayName = prevNode.children[0].value || pathwayName;
            console.log('Found pathway name from strong node:', pathwayName);
            break;
          }
          
          // Also check for text nodes that might contain the pathway name
          if (prevNode && prevNode.type === 'text' && prevNode.value) {
            // Look for text that ends with "**" (indicating it might be before a bold section)
            const textValue = prevNode.value.trim();
            if (textValue.length > 0 && !textValue.includes('http')) {
              pathwayName = textValue.replace(/\*+$/, '').trim();
              console.log('Found pathway name from text node:', pathwayName);
              break;
            }
          }
        }
      }
      
      // Approach 2: Check parent's parent (paragraph level)
      if (pathwayName === 'KEGG Pathway' && node && node.parent && node.parent.parent && node.parent.parent.children) {
        console.log('Trying paragraph level extraction');
        for (const child of node.parent.parent.children) {
          if (child.type === 'strong' && child.children && child.children[0] && child.children[0].value) {
            pathwayName = child.children[0].value;
            console.log('Found pathway name from paragraph level:', pathwayName);
            break;
          }
        }
      }
      
      // Fallback to URL extraction if no context found
      if (pathwayName === 'KEGG Pathway') {
        pathwayName = extractPathwayNameFromUrl(href, 'KEGG');
        console.log('Using fallback pathway name:', pathwayName);
      }
      
      console.log('Rendering KEGG PathwayCard with name:', pathwayName);
      return <PathwayCard pathwayName={pathwayName} url={href} type="KEGG" />;
    }
  }

  // Default link behavior for all other URLs
  console.log('Rendering default link for:', href);
  return (
    <a {...props} href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
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
      if (msg.section === 'Summary' || msg.section === 'Answer') {
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
                  <Box className="markdown-body">
                    <ReactMarkdown
                      remarkPlugins={[remarkGfm, remarkMath]}
                      rehypePlugins={[rehypeKatex]}
                      components={{
                        code: CodeBlock,
                        a: CustomLinkComponent
                      }}
                    >
                      {transformSummary(chunk.summaryText)}
                    </ReactMarkdown>
                  </Box>
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
function Chatbot({ selectedSessionId, onSessionChange = () => {}, onConversationUpdate = () => {} }) {
  const [messages, setMessages] = useState([]);
  const [newMessage, setNewMessage] = useState('');
  const [loading, setLoading] = useState(false);
  const [sessionId, setSessionId] = useState(null);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [spectrumMode, setSpectrumMode] = useState(false);
  const [conversationTitle, setConversationTitle] = useState('');
  const initializationComplete = useRef(false);

  // Handle selected session changes from sidebar
  useEffect(() => {
    if (selectedSessionId === 'NEW_CHAT_TRIGGER') {
      // Explicitly start new conversation
      startNewConversationInternal();
    } else if (selectedSessionId && selectedSessionId !== 'NEW_CHAT_TRIGGER') {
      loadConversation(selectedSessionId);
    } else if (selectedSessionId === null) {
      // Clear conversation but don't auto-create new one
      setMessages([]);
      setSessionId(null);
      setConversationTitle('');
      localStorage.removeItem('chatSessionId');
    }
  }, [selectedSessionId]);

  // Initialize chat session and load history when component mounts
  useEffect(() => {
    if (!selectedSessionId && !initializationComplete.current) {
      initializationComplete.current = true;
      initializeChat();
    }
  }, [selectedSessionId]);

  async function initializeChat() {
    try {
      // Try to load session ID from localStorage
      const savedSessionId = localStorage.getItem('chatSessionId');
      
      if (savedSessionId) {
        // Check if this session exists in conversations list
        const conversations = JSON.parse(localStorage.getItem('conversationList') || '[]');
        const existingConversation = conversations.find(conv => conv.id === savedSessionId);
        
        if (existingConversation) {
          setSessionId(savedSessionId);
          // Don't call onSessionChange here to avoid triggering useEffect loop
          
          // Load chat history for existing session
          setLoadingHistory(true);
          const history = api.loadConversationHistory(savedSessionId);
          if (history && history.length > 0) {
            setMessages(history);
            
            // Set conversation title from first message if available
            const firstUserMessage = history.find(msg => msg.role === 'user');
            if (firstUserMessage) {
              const title = api.generateConversationTitle(firstUserMessage.content);
              setConversationTitle(title);
            }
          } else {
            // Try loading from API as fallback
            const apiHistory = await api.getChatHistory(savedSessionId);
            if (apiHistory && apiHistory.length > 0) {
              setMessages(apiHistory);
              
              // Set conversation title from first message if available
              const firstUserMessage = apiHistory.find(msg => msg.role === 'user');
              if (firstUserMessage) {
                const title = api.generateConversationTitle(firstUserMessage.content);
                setConversationTitle(title);
              }
            }
          }
          setLoadingHistory(false);
        } else {
          // Session doesn't exist in conversations, clear it
          localStorage.removeItem('chatSessionId');
          setSessionId(null);
          setMessages([]);
        }
      } else {
        // No saved session, start with empty state
        setSessionId(null);
        setMessages([]);
      }
    } catch (error) {
      console.error('Error initializing chat:', error);
      setSessionId(null);
      setMessages([]);
    }
  }

  async function startNewConversation() {
    try {
      const { id } = await api.createChat();
      setSessionId(id);
      setMessages([]);
      setConversationTitle('');
      localStorage.setItem('chatSessionId', id);
      onSessionChange(id);
      // Don't save conversation here - it will be saved when user sends first message
    } catch (error) {
      console.error('Error starting new conversation:', error);
    }
  }

  async function startNewConversationInternal() {
    try {
      const { id } = await api.createChat();
      setSessionId(id);
      setMessages([]);
      setConversationTitle('');
      localStorage.setItem('chatSessionId', id);
      // Don't call onSessionChange here to avoid loop
      // Don't save conversation here - it will be saved when user sends first message
    } catch (error) {
      console.error('Error starting new conversation:', error);
    }
  }

  async function loadConversation(conversationId) {
    try {
      setLoadingHistory(true);
      setSessionId(conversationId);
      localStorage.setItem('chatSessionId', conversationId);
      
      // Load conversation history
      const history = api.loadConversationHistory(conversationId);
      if (history && history.length > 0) {
        setMessages(history);
        
        // Set conversation title from first message if available
        const firstUserMessage = history.find(msg => msg.role === 'user');
        if (firstUserMessage && !conversationTitle) {
          const title = api.generateConversationTitle(firstUserMessage.content);
          setConversationTitle(title);
        }
      } else {
        // Try loading from API as fallback
        const apiHistory = await api.getChatHistory(conversationId);
        if (apiHistory && apiHistory.length > 0) {
          setMessages(apiHistory);
          
          // Set conversation title from first message if available
          const firstUserMessage = apiHistory.find(msg => msg.role === 'user');
          if (firstUserMessage && !conversationTitle) {
            const title = api.generateConversationTitle(firstUserMessage.content);
            setConversationTitle(title);
          }
        } else {
          setMessages([]);
          setConversationTitle('');
        }
      }
    } catch (error) {
      console.error('Error loading conversation:', error);
      setMessages([]);
      setConversationTitle('');
    } finally {
      setLoadingHistory(false);
    }
  }

  async function submitNewMessage() {
    const trimmedMessage = newMessage.trim();
    if (!trimmedMessage || loading) return;
    
    // If no session exists, create one first
    if (!sessionId) {
      const { id } = await api.createChat();
      setSessionId(id);
      localStorage.setItem('chatSessionId', id);
      onSessionChange(id);
      
      // Continue with the current session id
      const currentSessionId = id;
      await processMessage(trimmedMessage, currentSessionId);
      return;
    }
    
    await processMessage(trimmedMessage, sessionId);
  }

  async function processMessage(trimmedMessage, currentSessionId) {
    const userMessage = { role: 'user', content: trimmedMessage };
    setMessages((prev) => [...prev, userMessage]);
    setNewMessage('');
    setLoading(true);

    // Generate conversation title if this is the first message
    // Use the updated messages length instead of stale state
    setMessages((currentMessages) => {
      if (currentMessages.length === 1) { // Just added the user message, so length is 1
        const title = api.generateConversationTitle(trimmedMessage);
        setConversationTitle(title);
      }
      return currentMessages;
    });

    try {
      let assistantMessages = [];
      
      for await (const event of api.sendChatMessage(currentSessionId, trimmedMessage, spectrumMode)) {
        // Check for session updates
        if (event.section === 'SessionUpdate' && event.sessionId) {
          setSessionId(event.sessionId);
          localStorage.setItem('chatSessionId', event.sessionId);
          onSessionChange(event.sessionId);
          continue;
        }
        
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
            const newAssistantMessage = {
              role: 'assistant',
              section: event.section,
              text: event.text || ''
            };
            updated.push(newAssistantMessage);
            assistantMessages.push(newAssistantMessage);
          }
          return updated;
        });
      }

      // Save conversation after successful completion
      setMessages((currentMessages) => {
        // Only save if we have actual messages
        if (currentMessages.length > 0) {
          const title = conversationTitle || api.generateConversationTitle(trimmedMessage);
          const savedConversation = api.saveConversation(currentSessionId, title, currentMessages);
          if (savedConversation) {
            // Use setTimeout to avoid triggering during render
            setTimeout(() => {
              onConversationUpdate(); // Notify parent to refresh sidebar
            }, 50);
            if (!conversationTitle) {
              setConversationTitle(title); // Update the title for this session
            }
          }
        }
        return currentMessages;
      });

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
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        overflow: 'hidden',
        bgcolor: 'background.default'
      }}
    >
      {loadingHistory ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', flexGrow: 1 }}>
          <CircularProgress size={40} />
          <Typography variant="body2" sx={{ ml: 2 }}>
            Loading conversation history...
          </Typography>
        </Box>
      ) : (
        <>
          <Box
            sx={{
              flexGrow: 1,
              overflowY: 'auto',
              display: 'flex',
              flexDirection: 'column'
            }}
          >
            <ChatMessages messages={messages} loading={loading} />
          </Box>

          <ChatInput
            newMessage={newMessage}
            setNewMessage={setNewMessage}
            submitNewMessage={submitNewMessage}
            isLoading={loading}
            spectrumMode={spectrumMode}
            setSpectrumMode={setSpectrumMode}
          />
        </>
      )}
    </Box>
  );
}

export default Chatbot;
