"""Tests for the Loguru-based logger module."""

from de_projet_perso.core.logger import logger


def test_should_work():  # noqa: D103
    assert logger == "test"
