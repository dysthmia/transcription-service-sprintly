from __future__ import annotations

import logging

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(log_level: str) -> None:
    """Настраивает стандартную систему логирования приложения."""

    # Получаем числовое значение уровня логирования.
    level = logging.getLevelNamesMapping().get(log_level.upper())
    
    # Проверяем, что указанный уровень существует.
    if level is None:
        raise ValueError(
            f"Неподдерживаемый уровень логирования: {log_level}"
        )

    logging.basicConfig(level=level, format=LOG_FORMAT)
