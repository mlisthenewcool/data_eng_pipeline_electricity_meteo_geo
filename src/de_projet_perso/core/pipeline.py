"""TODO."""

# from abc import ABC, abstractmethod
# from pathlib import Path
#
# from de_projet_perso.core.catalog import Dataset
#
#
# class BasePipeline(ABC):
#     def __init__(self, dataset: Dataset):
#         self.dataset = dataset
#
#     @abstractmethod
#     async def download(self) -> Path:
#         """Download raw data."""
#
#     @abstractmethod
#     async def extract(self, archive_path: Path) -> Path:
#         """Extract from archive if needed."""
#
#     @abstractmethod
#     async def transform(self, raw_path: Path) -> Path:
#         """Transform to target format."""
#
#     async def run(self) -> Path:
#         """Execute full pipeline."""
#         raw = await self.download()
#         extracted = await self.extract(raw)
#         return await self.transform(extracted)
