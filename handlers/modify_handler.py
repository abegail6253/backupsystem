"""Handler for watchdog on_modified events ("modified" event type)."""

import logging
from .base import BaseEventHandler

logger = logging.getLogger(__name__)


class ModifyEventHandler(BaseEventHandler):
    """Handles file-modification events detected by watchdog."""

    def handle(self, event) -> None:
        logger.debug(
            f"[watchdog.on_modified] is_directory={event.is_directory} "
            f"src={event.src_path!r}"
        )
        if not event.is_directory:
            self._record("modified", event.src_path)
