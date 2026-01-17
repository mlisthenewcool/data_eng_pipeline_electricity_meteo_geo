# noqa: D100
import statistics
import time
from pathlib import Path

from de_projet_perso.core.logger import logger
from de_projet_perso.utils.hasher import FileHasher


def benchmark_hash(path: Path, chunk_size: int, repeat: int = 3):
    """..."""
    speeds = []
    size_mb = path.stat().st_size / (1024 * 1024)

    for i in range(repeat):
        hasher = FileHasher()
        start_time = time.perf_counter()

        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hasher.update(chunk)

        duration = time.perf_counter() - start_time
        speeds.append(size_mb / duration)

    avg_speed = statistics.mean(speeds)
    std_dev = statistics.stdev(speeds) if repeat > 1 else 0

    logger.info(
        f"Chunk: {chunk_size:>7} o | "
        f"Vitesse Moyenne: {avg_speed:>7.2f} Mo/s | "
        f"(± {std_dev:.2f} sur {repeat} tests)"
    )


if __name__ == "__main__":
    test_file = Path("data/landing/ADMIN-EXPRESS-COG.7z")

    if test_file.exists():
        logger.info(f"Début du benchmark sur {test_file.name}...")
        for size in [4096, 16384, 65536, 131072, 1048576, 5242880]:  # 4K à 5M
            benchmark_hash(test_file, size, repeat=20)
    else:
        logger.error(f"Fichier non trouvé : {test_file}")
