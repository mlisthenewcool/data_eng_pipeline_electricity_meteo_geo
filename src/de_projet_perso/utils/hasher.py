"""TODO."""

import hashlib
from pathlib import Path

from de_projet_perso.core.exceptions import FileIntegrityError
from de_projet_perso.core.settings import DOWNLOAD_CHUNK_SIZE

HASH_ALGORITHM = "sha256"


class FileHasher:
    """Memory-efficient hashing for files and streams with integrity check."""

    def __init__(self, algorithm: str = HASH_ALGORITHM):
        """TODO."""
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
        cls, path: Path, algorithm: str = HASH_ALGORITHM, chunk_size: int = DOWNLOAD_CHUNK_SIZE
    ) -> str:
        """Hash an existing file using chunks to save RAM."""
        instance = cls(algorithm)
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                instance.update(chunk)
        return instance.hexdigest

    @classmethod
    def verify(cls, path: Path, expected_hash: str, algorithm: str = HASH_ALGORITHM) -> None:
        """Verify file integrity. Raises FileIntegrityError if hashes don't match."""
        actual = cls.hash_file(path, algorithm)
        if actual.lower() != expected_hash.lower():
            raise FileIntegrityError(
                path=path,
                reason=f"Hash mismatch ({algorithm}). Expected: {expected_hash}, Got: {actual}",
            )
