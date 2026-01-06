"""Metadata manager module for Video Compression Optimizer."""

from vco.metadata.embedder import EmbedResult, MetadataEmbedder
from vco.metadata.extractor import MetadataExtractor
from vco.metadata.manager import MetadataManager, VideoMetadata
from vco.metadata.verifier import MetadataVerifier

__all__ = [
    "EmbedResult",
    "MetadataEmbedder",
    "MetadataExtractor",
    "MetadataManager",
    "MetadataVerifier",
    "VideoMetadata",
]
