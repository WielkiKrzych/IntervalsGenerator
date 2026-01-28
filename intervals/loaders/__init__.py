# Loaders package
# Import registry first to avoid circular imports
from .registry import LoaderRegistry

# Import loaders (registration happens via decorators at import time)
from .trainred import TrainRedLoader
from .tymewear import TymewearLoader
from .wahoo import WahooLoader
from .garmin import GarminLoader

__all__ = [
    'LoaderRegistry',
    'TrainRedLoader',
    'TymewearLoader',
    'WahooLoader',
    'GarminLoader',
]
