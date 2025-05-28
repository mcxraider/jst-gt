# services/storage/exceptions.py
"""Custom exceptions for storage operations."""

class StorageError(Exception):
    """Base exception for storage operations."""
    pass

class S3Error(StorageError):
    """S3-specific storage errors."""
    pass

class LocalStorageError(StorageError):
    """Local filesystem storage errors."""
    pass

class ValidationError(StorageError):
    """Data validation errors."""
    pass