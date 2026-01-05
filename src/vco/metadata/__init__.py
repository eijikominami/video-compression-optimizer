"""Metadata manager module for Video Compression Optimizer."""

from vco.metadata.extractor import MetadataExtractor
from vco.metadata.manager import MetadataManager, VideoMetadata
from vco.metadata.verifier import MetadataVerifier

__all__ = ["MetadataExtractor", "MetadataManager", "MetadataVerifier", "VideoMetadata"]
