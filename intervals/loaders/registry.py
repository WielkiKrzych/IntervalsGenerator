"""
Plugin loader registry for dynamic loader registration.

This module provides a registry pattern for data source loaders,
allowing new loaders to be added without modifying existing code.

Usage:
    # Automatic registration via decorator
    @LoaderRegistry.register("new_source")
    class NewSourceLoader(BaseLoader):
        ...

    # Manual registration
    LoaderRegistry.register_loader("new_source", NewSourceLoader)

    # Get all loaders
    loaders = LoaderRegistry.get_all_loaders(config, fs, ui)

    # Get specific loader
    loader_class = LoaderRegistry.get_loader("trainred")
    loader = loader_class(config, fs, ui)

Example - Adding a new loader:

    # 1. Create new loader file: intervals/loaders/polar.py
    from .base import BaseLoader
    from .registry import LoaderRegistry

    @LoaderRegistry.register("polar")
    class PolarLoader(BaseLoader):
        @property
        def name(self) -> str:
            return "Polar"

        # ... implement other methods

    # 2. Import in __init__.py
    from .polar import PolarLoader

    # That's it! The loader is now available in the pipeline.
"""

from typing import Dict, Type, List, Optional, Any, Union
import logging

from .base import BaseLoader

logger = logging.getLogger(__name__)


class LoaderRegistry:
    """
    Registry for data source loaders.

    Provides decorator-based registration and factory methods
    for creating loader instances.

    Thread-safe: Uses class-level dictionary which is safe for reads
    during normal operation. Registration typically happens at import time.
    """

    _loaders: Dict[str, Type[BaseLoader]] = {}
    _metadata: Dict[str, Dict[str, Union[int, str, List[str]]]] = {}

    @classmethod
    def register(
        cls,
        name: str,
        priority: int = 100,
        description: str = "",
        file_patterns: Optional[List[str]] = None,
    ):
        """
        Decorator for registering loaders.

        Args:
            name: Unique identifier for the loader
            priority: Processing order (lower = earlier), default 100
            description: Human-readable description
            file_patterns: List of glob patterns this loader handles

        Returns:
            Decorator function

        Example:
            @LoaderRegistry.register("trainred", priority=10)
            class TrainRedLoader(BaseLoader):
                ...
        """

        def decorator(loader_class: Type[BaseLoader]) -> Type[BaseLoader]:
            if name in cls._loaders:
                logger.warning(
                    f"Loader '{name}' already registered. "
                    f"Overwriting {cls._loaders[name].__name__} with {loader_class.__name__}"
                )

            cls._loaders[name] = loader_class
            cls._metadata[name] = {
                "priority": priority,
                "description": description or loader_class.__doc__ or "",
                "file_patterns": file_patterns or [],
                "class_name": loader_class.__name__,
            }

            logger.debug(f"Registered loader: {name} -> {loader_class.__name__}")
            return loader_class

        return decorator

    @classmethod
    def register_loader(
        cls, name: str, loader_class: Type[BaseLoader], priority: int = 100
    ) -> None:
        """
        Manually register a loader class.

        Args:
            name: Unique identifier for the loader
            loader_class: The loader class to register
            priority: Processing order (lower = earlier)
        """
        cls._loaders[name] = loader_class
        cls._metadata[name] = {
            "priority": priority,
            "description": loader_class.__doc__ or "",
            "file_patterns": [],
            "class_name": loader_class.__name__,
        }

    @classmethod
    def unregister(cls, name: str) -> bool:
        """
        Unregister a loader.

        Args:
            name: Name of loader to remove

        Returns:
            True if loader was removed, False if not found
        """
        if name in cls._loaders:
            del cls._loaders[name]
            del cls._metadata[name]
            return True
        return False

    @classmethod
    def get_loader(cls, name: str) -> Type[BaseLoader]:
        """
        Get loader class by name.

        Args:
            name: Loader identifier

        Returns:
            Loader class

        Raises:
            KeyError: If loader not found
        """
        if name not in cls._loaders:
            available = ", ".join(cls._loaders.keys())
            raise KeyError(
                f"Loader '{name}' not registered. Available loaders: {available}"
            )
        return cls._loaders[name]

    @classmethod
    def get_loader_safe(cls, name: str) -> Optional[Type[BaseLoader]]:
        """
        Get loader class by name, or None if not found.

        Args:
            name: Loader identifier

        Returns:
            Loader class or None
        """
        return cls._loaders.get(name)

    @classmethod
    def get_all_loaders(cls, config, fs, ui) -> List[BaseLoader]:
        """
        Instantiate all registered loaders, sorted by priority.

        Args:
            config: Configuration object
            fs: FileSystem operations object
            ui: UserInterface object

        Returns:
            List of loader instances, sorted by registration priority
        """
        # Sort by priority
        sorted_names = sorted(
            cls._loaders.keys(),
            key=lambda n: cls._metadata.get(n, {}).get("priority", 100),
        )

        instances = []
        for name in sorted_names:
            try:
                loader_class = cls._loaders[name]
                instance = loader_class(config, fs, ui)
                instances.append(instance)
            except Exception as e:
                logger.error(f"Failed to instantiate loader '{name}': {e}")

        return instances

    @classmethod
    def available_loaders(cls) -> List[str]:
        """
        List all registered loader names.

        Returns:
            List of loader names, sorted by priority
        """
        return sorted(
            cls._loaders.keys(),
            key=lambda n: cls._metadata.get(n, {}).get("priority", 100),
        )

    @classmethod
    def get_metadata(cls, name: str) -> Dict[str, Any]:
        """
        Get metadata for a registered loader.

        Args:
            name: Loader identifier

        Returns:
            Metadata dictionary with priority, description, etc.
        """
        return cls._metadata.get(name, {})

    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        """
        List all loaders with their metadata.

        Returns:
            List of dictionaries with name, class, and metadata
        """
        result = []
        for name in cls.available_loaders():
            meta = cls._metadata.get(name, {})
            result.append(
                {
                    "name": name,
                    "class": cls._loaders[name].__name__,
                    "priority": meta.get("priority", 100),
                    "description": meta.get("description", ""),
                }
            )
        return result

    @classmethod
    def clear(cls) -> None:
        """
        Clear all registered loaders.
        Primarily useful for testing.
        """
        cls._loaders.clear()
        cls._metadata.clear()

    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check if a loader is registered."""
        return name in cls._loaders
