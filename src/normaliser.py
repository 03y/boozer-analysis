import json
import time
from typing import Optional, Dict, Any
from openai import OpenAI
from dataclasses import dataclass


@dataclass
class LLMNormalizerConfig:
    model: str = "gpt-4o-mini"           # You can swap for larger models
    temperature: float = 0.0            # Deterministic output
    max_retries: int = 3
    delay_between_retries: float = 1.0  # seconds


class LLMNormalizer:
    def __init__(self, config: LLMNormalizerConfig):
        self.client = OpenAI()
        self.config = config

    def _build_prompt(self, name: str, units: Optional[float]) -> str:
        return f"""
Your task is to normalize and classify the following drink item.

User-entered item:
Name: "{name}"
Units: {units}

Return ONLY valid JSON with the following schema:

{{
  "canonical_name": "",
  "drink_type": "",
  "subtype": "",
  "volume_ml": null,
  "abv_percent": null,
  "brand": "",
  "container_type": "",
  "is_valid_drink": true,
  "confidence": 0.0
}}

Rules:
- If unsure, leave fields null or empty but maintain JSON structure.
- Always output valid JSON only.
"""

    def _call_llm(self, prompt: str) -> str:
        """Low-level API call with retry logic."""
        for attempt in range(self.config.max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.config.model,
                    temperature=self.config.temperature,
                    messages=[{"role": "system", "content": prompt}]
                )
                return response.choices[0].message["content"]

            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise e
                time.sleep(self.config.delay_between_retries)

    def _parse_json(self, text: str) -> Dict[str, Any]:
        """Extract JSON object from the LLM response."""
        try:
            # If the model returns raw JSON, this works directly
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to extract JSON inside text
            try:
                start = text.index("{")
                end = text.rindex("}") + 1
                json_str = text[start:end]
                return json.loads(json_str)
            except Exception:
                raise ValueError("LLM response did not contain valid JSON:\n" + text)

    def normalize_item(self, name: str, units: Optional[float] = None) -> Dict[str, Any]:
        """Public method: input->prompt->LLM->parsed JSON."""
        prompt = self._build_prompt(name, units)
        raw_output = self._call_llm(prompt)
        return self._parse_json(raw_output)
