const API_URL = 'http://localhost:8001';

export async function* parseSSEStream(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let eventLines = [];

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      // Append the new chunk to our buffer.
      buffer += decoder.decode(value, { stream: true });
      // Split the buffer by newlines.
      const lines = buffer.split('\n');
      // The last line may be incomplete so we save it back to buffer.
      buffer = lines.pop();

      for (const rawLine of lines) {
        const line = rawLine.trim();
        // An empty line indicates the end of an event.
        if (line === '') {
          if (eventLines.length > 0) {
            // Combine all lines that start with "data:".
            const dataParts = eventLines
              .filter(l => l.startsWith('data:'))
              .map(l => l.slice(5).trim());
            const dataString = dataParts.join('\n');
            eventLines = []; // Reset for the next event.
            try {
              // Try parsing the JSON string.
              const parsedData = JSON.parse(dataString);
              yield parsedData;
            } catch (error) {
              // If parsing fails, yield the raw string.
              yield dataString;
            }
          }
        } else {
          // Otherwise, accumulate the line.
          eventLines.push(line);
        }
      }
    }
    // Process any remaining lines after the stream ends.
    if (buffer.trim() !== '') {
      eventLines.push(buffer.trim());
    }
    if (eventLines.length > 0) {
      const dataParts = eventLines
        .filter(l => l.startsWith('data:'))
        .map(l => l.slice(5).trim());
      if (dataParts.length > 0) {
        const dataString = dataParts.join('\n');
        try {
          const parsedData = JSON.parse(dataString);
          yield parsedData;
        } catch (error) {
          yield dataString;
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

export default {
  createChat: async () => {
    try {
      const response = await fetch(`${API_URL}/session`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      
      if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
      }
      
      const data = await response.json();
      return { id: data.session_id };
    } catch (error) {
      console.error('Error creating chat session:', error);
      // Fallback to default ID if API fails
      return { id: 'default-chat' };
    }
  },

  // sendChatMessage now automatically processes the SSE stream.
  sendChatMessage: async function* (chatId, message) {
    const response = await fetch(`${API_URL}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 
        question: message,
        session_id: chatId 
      }),
    });
    
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    
    // Check if a new session ID was assigned
    const newSessionId = response.headers.get('X-Session-ID');
    if (newSessionId && newSessionId !== chatId) {
      // Return session ID update as first event
      yield { 
        section: "SessionUpdate",
        sessionId: newSessionId
      };
    }
    
    yield* parseSSEStream(response.body);
  },

  getChatHistory: async (chatId) => {
    try {
      if (!chatId || chatId === 'default-chat') {
        return [];
      }
      
      const response = await fetch(`${API_URL}/memory/${chatId}`, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      });
      
      if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
      }
      
      const data = await response.json();
      
      // Transform memory format to chat format
      return data.turns.map(turn => ([
        { role: 'user', content: turn.user_query },
        { role: 'assistant', section: 'Answer', text: turn.answer }
      ])).flat();
    } catch (error) {
      console.error('Error fetching chat history:', error);
      return [];
    }
  }
};
