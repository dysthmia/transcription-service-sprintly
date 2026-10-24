from __future__ import annotations


class TranscriptError(Exception):
    """Базовое исключение с постоянным кодом ошибки и безопасным сообщением для потребителей событий."""
    def __init__(self, error_code: str, user_message: str) -> None:
        self.error_code = error_code
        self.user_message = user_message
        super().__init__(f"{error_code}: {user_message}")


class ContentError(TranscriptError):
    """Ошибка источника или содержимого, при которой повторная обработка не требуется."""


class TransientExternalError(TranscriptError):
    """Временная ошибка внешнего сервиса, при которой можно повторить обработку."""
