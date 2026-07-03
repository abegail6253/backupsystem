# Event handler package — one module per watchdog event type.
#
# watcher.py routes watchdog events here:
#   on_created  → add_handler.AddEventHandler
#   on_deleted  → delete_handler.DeleteEventHandler
#   on_modified → modify_handler.ModifyEventHandler
#   on_moved    → rename_handler.RenameEventHandler
#
# Shared pipeline (_is_excluded, _record, SMB snapshot, pending queue,
# history persist, suppression hook) lives in base.py.
from .add_handler    import AddEventHandler
from .delete_handler import DeleteEventHandler
from .modify_handler import ModifyEventHandler
from .rename_handler import RenameEventHandler

__all__ = [
    "AddEventHandler",
    "DeleteEventHandler",
    "ModifyEventHandler",
    "RenameEventHandler",
]
