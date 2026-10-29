from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.exceptions import ContentError, TransientExternalError
from app.features.summarization.service import SummarizationService


class FakeDeepSeekClient:
    def __init__(self, responses: list[str | Exception]) -> None:
        self._responses = iter(responses)
        self.prompts: list[str] = []

    async def complete_text(self, prompt: str) -> str:
        self.prompts.append(prompt)
        response = next(self._responses)
        if isinstance(response, Exception):
            raise response
        return response


def settings(chunk_size: int = 100) -> SimpleNamespace:
    return SimpleNamespace(summary_chunk_size=chunk_size)


@pytest.mark.asyncio
async def test_short_text_uses_one_russian_summary_request() -> None:
    client = FakeDeepSeekClient(["# Конспект\n\nПодробное объяснение."])
    service = SummarizationService(settings(), client)

    result = await service.summarize("English source text")

    assert result == "# Конспект\n\nПодробное объяснение."
    assert len(client.prompts) == 1
    assert "русском языке" in client.prompts[0]
    assert "English source text" in client.prompts[0]


@pytest.mark.asyncio
async def test_long_text_summarizes_chunks_then_merges_in_source_order() -> None:
    first_chunk = "Первый абзац. " * 5
    second_chunk = "Второй абзац. " * 5
    client = FakeDeepSeekClient(
        [
            "# Первая часть\n\nСодержание первой части.",
            "# Вторая часть\n\nСодержание второй части.",
            "# Итог\n\nОбъединённый конспект.",
        ]
    )
    service = SummarizationService(settings(chunk_size=100), client)

    result = await service.summarize(f"{first_chunk}\n\n{second_chunk}")

    assert result == "# Итог\n\nОбъединённый конспект."
    assert len(client.prompts) == 3
    merge_prompt = client.prompts[-1]
    assert merge_prompt.index("Содержание первой части.") < merge_prompt.index(
        "Содержание второй части."
    )


@pytest.mark.asyncio
async def test_chunked_summary_keeps_table_and_latex_in_merge_input() -> None:
    source = "Первый короткий абзац.\n\nВторой короткий абзац."
    table_and_formula = "| Величина | Значение |\n| --- | --- |\n| Энергия | $E=mc^2$ |"
    client = FakeDeepSeekClient(
        [
            table_and_formula,
            "# Вторая часть\n\nДополнительное пояснение.",
            "# Итог\n\n" + table_and_formula,
        ]
    )
    service = SummarizationService(settings(chunk_size=30), client)

    result = await service.summarize(source)

    assert table_and_formula in client.prompts[-1]
    assert table_and_formula in result


@pytest.mark.asyncio
@pytest.mark.parametrize("response", ["", " \n\t "])
async def test_empty_model_result_is_invalid_output(response: str) -> None:
    service = SummarizationService(settings(), FakeDeepSeekClient([response]))

    with pytest.raises(ContentError) as error:
        await service.summarize("Материал")

    assert error.value.error_code == "llm_invalid_output"


@pytest.mark.asyncio
async def test_invalid_deepseek_response_is_mapped_to_invalid_output() -> None:
    client = FakeDeepSeekClient(
        [ContentError("invalid_model_output", "Некорректный ответ.")]
    )
    service = SummarizationService(settings(), client)

    with pytest.raises(ContentError) as error:
        await service.summarize("Материал")

    assert error.value.error_code == "llm_invalid_output"


@pytest.mark.asyncio
async def test_temporary_deepseek_failure_is_retryable_llm_unavailable() -> None:
    client = FakeDeepSeekClient(
        [TransientExternalError("deepseek_temporary_failure", "Недоступен.")]
    )
    service = SummarizationService(settings(), client)

    with pytest.raises(TransientExternalError) as error:
        await service.summarize("Материал")

    assert error.value.error_code == "llm_unavailable"
