"""S3 key construction utility.

Centralizes all S3 key construction logic for consistency
between CLI and Lambda functions.

Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6
"""

from pathlib import Path


class S3KeyBuilder:
    """Builder for S3 keys used in async workflow.

    This class provides static methods for constructing S3 keys
    in a consistent format across CLI and Lambda functions.

    Key formats (aligned with existing implementation):
    - Source: async/{task_id}/input/{file_id}/{filename}
    - Output: output/{task_id}/{file_id}/{stem}_h265.mp4
    - Metadata: async/{task_id}/input/{file_id}/metadata.json
    """

    @staticmethod
    def source_key(task_id: str, file_id: str, filename: str) -> str:
        """Build S3 key for source file.

        Args:
            task_id: Task identifier (UUID)
            file_id: File identifier (UUID)
            filename: Original filename

        Returns:
            S3 key in format: async/{task_id}/input/{file_id}/{filename}
        """
        return f"async/{task_id}/input/{file_id}/{filename}"

    @staticmethod
    def output_key(task_id: str, file_id: str, filename: str) -> str:
        """Build S3 key for output file.

        Args:
            task_id: Task identifier (UUID)
            file_id: File identifier (UUID)
            filename: Original filename (used to extract stem)

        Returns:
            S3 key in format: output/{task_id}/{file_id}/{stem}_h265.mp4
        """
        stem = Path(filename).stem
        return f"output/{task_id}/{file_id}/{stem}_h265.mp4"

    @staticmethod
    def metadata_key(task_id: str, file_id: str, filename: str) -> str:
        """Build S3 key for metadata file.

        Args:
            task_id: Task identifier (UUID)
            file_id: File identifier (UUID)
            filename: Original filename (not used, kept for API consistency)

        Returns:
            S3 key in format: async/{task_id}/input/{file_id}/metadata.json
        """
        return f"async/{task_id}/input/{file_id}/metadata.json"

    @staticmethod
    def parse_source_key(source_key: str) -> tuple[str, str, str]:
        """Parse source S3 key to extract task_id, file_id, filename.

        Args:
            source_key: S3 key in format async/{task_id}/input/{file_id}/{filename}

        Returns:
            Tuple of (task_id, file_id, filename)

        Raises:
            ValueError: If key format is invalid
        """
        parts = source_key.split("/")
        # async/{task_id}/input/{file_id}/{filename}
        if len(parts) >= 5 and parts[0] == "async" and parts[2] == "input":
            return parts[1], parts[3], parts[4]
        raise ValueError(f"Invalid source key format: {source_key}")

    @staticmethod
    def parse_output_key(output_key: str) -> tuple[str, str, str]:
        """Parse output S3 key to extract task_id, file_id, filename.

        Args:
            output_key: S3 key in format output/{task_id}/{file_id}/{filename}

        Returns:
            Tuple of (task_id, file_id, filename)

        Raises:
            ValueError: If key format is invalid
        """
        parts = output_key.split("/")
        # output/{task_id}/{file_id}/{filename}
        if len(parts) >= 4 and parts[0] == "output":
            return parts[1], parts[2], parts[3]
        raise ValueError(f"Invalid output key format: {output_key}")

    @staticmethod
    def parse_metadata_key(metadata_key: str) -> tuple[str, str, str]:
        """Parse metadata S3 key to extract task_id, file_id.

        Args:
            metadata_key: S3 key in format async/{task_id}/input/{file_id}/metadata.json

        Returns:
            Tuple of (task_id, file_id, "metadata.json")

        Raises:
            ValueError: If key format is invalid
        """
        parts = metadata_key.split("/")
        # async/{task_id}/input/{file_id}/metadata.json
        if len(parts) >= 5 and parts[0] == "async" and parts[2] == "input":
            return parts[1], parts[3], parts[4]
        raise ValueError(f"Invalid metadata key format: {metadata_key}")
