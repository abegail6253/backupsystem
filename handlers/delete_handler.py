"""Handler for watchdog on_deleted events ("deleted" event type).

Deletion events get special treatment in _record():
  - SMB NetSessionEnum is retried up to 5 times (vs 1 for other types)
    because the session may close within ~300 ms of the delete completing.
  - A directory deletion is logged but not recorded (watchdog fires for dirs too).
"""

import logging
from .base import BaseEventHandler

logger = logging.getLogger(__name__)


class DeleteEventHandler(BaseEventHandler):
    """Handles file-deletion events detected by watchdog."""

    def handle(self, event) -> None:
        logger.debug(
            f"[watchdog.on_deleted] is_directory={event.is_directory} "
            f"src={event.src_path!r}"
        )
        if not event.is_directory:
            logger.warning(
                f"[watchdog] DELETE event fired for: {event.src_path!r} "
                f"(is_directory={event.is_directory})"
            )
            self._record("deleted", event.src_path)
        else:
            logger.debug(
                f"[watchdog.on_deleted] IGNORED (is_directory=True): "
                f"{event.src_path!r}"
            )
