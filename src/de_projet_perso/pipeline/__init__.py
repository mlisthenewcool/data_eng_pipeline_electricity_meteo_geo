"""Pipeline business logic modules.

This package contains pure business logic for data pipelines,
completely decoupled from Airflow orchestration.

Modules:
    decision: Pipeline action decision logic
    downloader: Download and extraction logic
    transformer: Bronze and silver transformation logic
    validator: State validation logic

Each module can be tested independently without Airflow dependencies.
"""

# from de_projet_perso.pipeline.decision import PipelineDecisionEngine
# from de_projet_perso.pipeline.downloader import DownloadResult, PipelineDownloader
# from de_projet_perso.pipeline.transformer import PipelineTransformer, TransformResult
# from de_projet_perso.pipeline.validator import PipelineValidator, ValidationResult
#
# __all__ = [
#     # Classes
#     "PipelineDecisionEngine",
#     "PipelineDownloader",
#     "PipelineTransformer",
#     "PipelineValidator",
#     # Data classes
#     "DownloadResult",
#     "TransformResult",
#     "ValidationResult",
# ]
