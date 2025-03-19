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
  createChat: async () => ({ id: 'default-chat' }),

  // sendChatMessage now automatically processes the SSE stream.
  sendChatMessage: async function* (chatId, message) {
    const response = await fetch(`${API_URL}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: message }),
    });
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    yield* parseSSEStream(response.body);
  },

  getChatHistory: async () => []
};
