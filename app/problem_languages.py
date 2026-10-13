"""Public statement translations and separately protected editorial text."""
import re
from pydantic import BaseModel, ConfigDict, Field, field_validator

ALIASES = {"korean": "ko", "english": "en", "japanese": "ja", "chinese": "zh", "russian": "ru",
           "german": "de", "french": "fr", "spanish": "es", "portuguese": "pt"}


def normalize_language(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,99}", value):
        raise ValueError("지문 언어는 영문 언어 코드로 지정하세요.")
    value = ALIASES.get(value.lower(), value).replace("_", "-")
    parts = value.split("-")
    return "-".join([parts[0].lower(), *(
        part.title() if len(part) == 4 and part.isalpha()
        else part.upper() if len(part) == 2 and part.isalpha()
        else part.lower() for part in parts[1:])])


class StatementTranslation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field(default="", max_length=255)
    statement: str = Field(default="", max_length=4 * 1024 * 1024)


class ProblemStatements(BaseModel):
    default_statement_language: str = "ko"
    statement_translations: dict[str, StatementTranslation] = Field(default_factory=dict, max_length=20)
    editorial_translations: dict[str, str] = Field(default_factory=dict, max_length=20)

    @field_validator("default_statement_language", mode="before")
    @classmethod
    def language(cls, value):
        return normalize_language(value)

    @field_validator("statement_translations", "editorial_translations", mode="before")
    @classmethod
    def language_keys(cls, value):
        if not isinstance(value, dict):
            raise ValueError("언어별 자료는 객체 형식이어야 합니다.")
        result = {}
        for language, item in value.items():
            key = normalize_language(language)
            if key in result:
                raise ValueError("같은 지문 언어를 중복 등록할 수 없습니다.")
            result[key] = item
        return result

    @field_validator("statement_translations", "editorial_translations")
    @classmethod
    def translation_size(cls, value):
        size = sum(len((item.statement if isinstance(item, StatementTranslation) else item).encode("utf-8"))
                   for item in value.values())
        if size > 4 * 1024 * 1024:
            raise ValueError("번역 자료의 전체 크기는 종류별 4MiB 이하로 제한합니다.")
        return value


def validate_statement_fields(default_language, translations, editorials):
    result = ProblemStatements(default_statement_language=default_language,
        statement_translations=translations, editorial_translations=editorials)
    if result.default_statement_language in result.statement_translations or result.default_statement_language in result.editorial_translations:
        raise ValueError("기본 언어는 기본 지문·해설에 저장하세요. 번역 언어와 중복할 수 없습니다.")
    return result.model_dump()
