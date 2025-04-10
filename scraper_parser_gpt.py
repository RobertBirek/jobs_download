import os
import openai
import logging
import json
import re
from typing import Optional, Union, List
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

####################################################
class ParsedOffer(BaseModel):
    experience_description: Optional[str] = None
    years_of_experience: Optional[str] = None
    interview_mode: Optional[str] = None
    position_title: Optional[str] = None
    position_level: Optional[str] = None
    responsibilities: Optional[Union[str, List[str]]] = None
    requirements: Optional[Union[str, List[str]]] = None
    benefits: Optional[Union[str, List[str]]] = None
    industry: Optional[str] = None
    company_size: Optional[str] = None

    @field_validator("responsibilities", "requirements", "benefits")
    @classmethod
    def convert_list_to_string(cls, v):
        if isinstance(v, list):
            return "\n".join(v)
        return v
####################################################
class OfferParserGPT:
    def __init__(self, raw_text: str):
        self.raw_text = raw_text
    ####################################################
    def parse(self) -> Optional[ParsedOffer]:
        from openai import OpenAI
        client = OpenAI()

        prompt = self._build_prompt(self.raw_text)

        try:
            response = client.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[
                    {"role": "system", "content": "Jesteś asystentem analizującym oferty pracy i wyciągającym informacje w języku polskim."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=1500
            )

            content = response.choices[0].message.content
            logging.info(f"Odpowiedź OpenAI:\n{content}")

            content = content.strip().removeprefix("```json").removesuffix("```").strip()

            try:
                json_part = re.search(r'\{.*\}', content, re.DOTALL)
                if json_part:
                    parsed_dict = json.loads(json_part.group(0))
                    return ParsedOffer(**parsed_dict)
                else:
                    raise ValueError("Nie znaleziono poprawnego fragmentu JSON")
            except Exception as parse_err:
                logging.error(f"Błąd parsowania JSON: {parse_err}\nZawartość:\n{content}")
                return None

        except Exception as e:
            logging.error(f"Błąd podczas komunikacji z OpenAI: {e}")
            return None
    ####################################################
    def _build_prompt(self, text: str) -> str:
        return f"""
Na podstawie poniższego opisu oferty pracy, uzupełnij następujące pola i zwróć wynik jako JSON (po polsku):

Ogranicz maksymalną długość każdego pola do:
- experience_description: 300 znaków
- years_of_experience: 20 znaków
- interview_mode: 50 znaków
- position_title: 100 znaków
- position_level: 50 znaków
- responsibilities: 500 znaków
- requirements: 500 znaków
- benefits: 300 znaków
- industry: 100 znaków
- company_size: 100 znaków

Jeśli nie potrafisz znaleźć danej informacji w opisie, pozostaw pole puste.
Upewnij się, że JSON jest poprawny, nie zawiera niedomkniętych ciągów ani przecinków na końcu.

Oferta:
{text}
"""
