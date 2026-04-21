"""Base class for all experiments."""

from abc import ABC, abstractmethod
from typing import List, Optional

from ..models import ExperimentCategory, RunConfig


class Experiment(ABC):
    name: str
    category: ExperimentCategory
    description: str

    @abstractmethod
    def generate_runs(
        self,
        protocols: Optional[List[str]] = None,
    ) -> List[RunConfig]:
        """Generate all RunConfig objects for this experiment's parameter sweep.

        Args:
            protocols: Override the default protocol list. If None, uses the
                       experiment's default protocols.
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, category={self.category.value!r})"
