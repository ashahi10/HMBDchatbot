/**
 * Asynchronously parse a stream of Server-Sent Events (SSE)
 * Each event is expected to be a JSON string prefixed with "data:"
 *
 * @param {ReadableStream} stream - The response body stream from the backend.
 * @returns {AsyncGenerator<Object>} Yields each parsed JSON object.
 */
export async function* parseSSEStream(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let lastYieldedEvent = null; // Track the last event to avoid duplicates

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split('\n\n');
      buffer = parts.pop() || '';

      for (const part of parts) {
        if (part.startsWith('data:')) {
          const dataString = part.slice(5).trim();
          if (dataString === lastYieldedEvent) continue;
          lastYieldedEvent = dataString;
          try {
            const parsed = JSON.parse(dataString);
            // console.log('parsed-parts', parsed);
            yield parsed;
          } catch (e) {
            console.error("Failed to parse SSE event:", e, dataString);
          }
        }
      }
    }
    if (buffer.startsWith('data:')) {
      const dataString = buffer.slice(5).trim();
      if (dataString !== lastYieldedEvent) {
        try {
          const parsed = JSON.parse(dataString);
          // console.log('parsed-buffer', parsed);
          yield parsed;
        } catch (e) {
          console.error("Failed to parse final SSE event:", e, dataString);
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
