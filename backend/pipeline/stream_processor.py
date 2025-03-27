import json
from typing import List, AsyncGenerator, Dict, Any

BAD_RESPONSES = ["```", "json", "```json", "```cypher", "```cypher\n", "```", "cy", "pher", "``"]

class StreamProcessor:
    @staticmethod
    def format_message(section: str, text: str) -> str:
        message = {"section": section, "text": text}
        return f"data:{json.dumps(message)}\n\n"

    @staticmethod
    async def process_stream( chain: Any, section: str, inputs: Dict[str, Any], accumulator: List[str] ) -> AsyncGenerator[str, None]:
        buffer = ""
        async for chunk in chain.astream(inputs):
            if not chunk:
                continue
            chunk_text = str(chunk)
            buffer += chunk_text
            while "<think>" in buffer and "</think>" in buffer:
                pre, _, remainder = buffer.partition("<think>")
                thinking, _, post = remainder.partition("</think>")
                if thinking.strip():
                    yield StreamProcessor.format_message("Thinking", thinking.strip())
                buffer = pre + post
            if buffer and "<think>" not in buffer:
                yield StreamProcessor.format_message(section, buffer)
                if section != "Thinking" and buffer not in BAD_RESPONSES + ["DONE"]:
                    accumulator.append(buffer)
                buffer = ""
        if buffer:
            if "<think>" in buffer and "</think>" in buffer:
                thinking = buffer.split("<think>")[1].split("</think>")[0].strip()
                if thinking:
                    yield StreamProcessor.format_message("Thinking", thinking)
            else:
                yield StreamProcessor.format_message(section, buffer)
                if section != "Thinking" and buffer not in BAD_RESPONSES + ["DONE"]:
                    accumulator.append(buffer)
        yield StreamProcessor.format_message(section, "DONE") 