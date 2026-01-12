# noqa: D100
from de_projet_perso.core.logger import _test_logs_visually


def test_import_in_dags(x: int) -> int:  # noqa: D103
    return x**2


def test_with_custom_logger() -> None:  # noqa: D103
    _test_logs_visually()
