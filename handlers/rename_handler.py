"""Handler for watchdog on_moved events ("renamed" event type).

Special case: if the destination path matches an exclude pattern (e.g. the
file is renamed to a temp/hidden name like file.tmp), the event is recorded
as "deleted" rather than "renamed" so the source file is not shown as still
present under its old name.
"""

import logging
from .base import BaseEventHandler

logger = logging.getLogger(__name__)


class RenameEventHandler(BaseEventHandler):
    """Handles file-rename/move events detected by watchdog."""

    def handle(self, event) -> None:
        logger.info(
            f"[watchdog] on_moved: is_directory={event.is_directory} "
            f"src={event.src_path!r} "
            f"dest={getattr(event, 'dest_path', None)!r} "
            f"(watch_id={self.watch_id!r})"
        )
        if not event.is_directory:
            dest = getattr(event, "dest_path", None)
            # If renamed INTO an excluded pattern (e.g. file.tmp), record as deleted
            if dest and self._is_excluded(dest):
                logger.info(
                    f"[watchdog] RENAME→EXCLUDED: recording as deleted: "
                    f"{event.src_path!r}"
                )
                self._record("deleted", event.src_path)
            else:
                self._record("renamed", event.src_path, dest)
