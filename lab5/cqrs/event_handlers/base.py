from abc import ABC, abstractmethod
from cqrs.events import StoredEvent


class EventHandler(ABC):
    """Base class for event handlers that process domain events"""
    
    @abstractmethod
    def can_handle(self, event: StoredEvent) -> bool:
        """Check if this handler can process the given event"""
        pass
    
    @abstractmethod
    def handle(self, event: StoredEvent) -> None:
        """Process the event (update projections, invalidate cache, etc.)"""
        pass
