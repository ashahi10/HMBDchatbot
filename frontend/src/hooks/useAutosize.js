import { useEffect, useRef } from 'react';

export default function useAutosize(text) {
  const textareaRef = useRef(null);
  
  useEffect(() => {
    if (!textareaRef.current) return;
    const textarea = textareaRef.current;
    textarea.style.height = 'auto';
    textarea.style.height = `${textarea.scrollHeight}px`;
  }, [text]);
  
  return textareaRef;
}