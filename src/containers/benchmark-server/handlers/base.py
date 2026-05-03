"""Base handler — abstract pattern for Blueprint registration."""

from abc import ABC, abstractmethod
from flask import Flask


class BaseHandler(ABC):
    """Abstract base for all route handlers."""

    @abstractmethod
    def register(self, app: Flask) -> None:
        """Register routes on the Flask app (via Blueprint)."""
        ...
