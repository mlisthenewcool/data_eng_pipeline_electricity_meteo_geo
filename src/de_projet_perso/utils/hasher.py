"""File hashing utilities with memory-efficient streaming.

Provides SHA256 (and other algorithms) hashing for files and byte streams,
with configurable chunk sizes for memory efficiency.
"""

import hashlib
from pathlib import Path

from de_projet_perso.core.settings import settings


class FileHasher:
    """Memory-efficient hashing for files and streams with integrity check."""

    def __init__(self, algorithm: str = settings.hash_algorithm):
        """Initialize hasher with specified algorithm.

        Args:
            algorithm: Hash algorithm name (default from settings)
        """
        self._hasher = hashlib.new(algorithm)
        self.algorithm = algorithm

    def update(self, chunk: bytes) -> None:
        """Update hash with a new chunk of data (streaming mode)."""
        self._hasher.update(chunk)

    @property
    def hexdigest(self) -> str:
        """Return the final hexadecimal hash."""
        return self._hasher.hexdigest()

    @classmethod
    def hash_file(
        cls,
        path: Path,
        algorithm: str = settings.hash_algorithm,
        chunk_size: int = settings.hash_chunk_size,
    ) -> str:
        """Hash an existing file using chunks to save RAM."""
        instance = cls(algorithm)
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                instance.update(chunk)
        return instance.hexdigest

    # @classmethod
    # def verify(
    #     cls, path: Path, expected_hash: str, algorithm: str = settings.hash_algorithm
    # ) -> None:
    #     """Verify file integrity. Raises FileIntegrityError if hashes don't match."""
    #     actual = cls.hash_file(path, algorithm)
    #     if actual.lower() != expected_hash.lower():
    #         raise FileIntegrityError(
    #             path=path,
    #             reason=f"Hash mismatch ({algorithm}). Expected: {expected_hash}, Got: {actual}",
    #         )
