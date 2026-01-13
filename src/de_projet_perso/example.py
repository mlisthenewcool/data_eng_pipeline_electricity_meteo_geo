# noqa: D100
import asyncio

from de_projet_perso.core.downloader import _test_download
from de_projet_perso.core.logger import _test_logs_visually


def test_import_in_dags(x: int) -> int:  # noqa: D103
    return x**2


def test_with_custom_logger_in_dags() -> None:  # noqa: D103
    _test_logs_visually()


def test_download_file_in_dags() -> None:  # noqa: D103
    asyncio.run(_test_download())
