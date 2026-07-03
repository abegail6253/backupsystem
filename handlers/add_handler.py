"""Handler for watchdog on_created events ("added" event type)."""

import logging
from .base import BaseEventHandler

logger = logging.getLogger(__name__)


class AddEventHandler(BaseEventHandler):
    """Handles file-creation events detected by watchdog."""

    def handle(self, event) -> None:
        logger.debug(
            f"[watchdog.on_created] is_directory={event.is_directory} "
            f"src={event.src_path!r}"
        )
        if not event.is_directory:
            self._record("added", event.src_path)
