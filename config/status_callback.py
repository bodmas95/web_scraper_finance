"""
============================================================
  STATUS CALLBACK FOR UI UPDATES
============================================================
  Allows agents to update UI status without circular imports
============================================================
"""

# Global callback function
_status_callback = None


def set_status_callback(callback_func):
    """Set the global status callback function"""
    global _status_callback
    _status_callback = callback_func


def update_status(message: str, status_type: str = "info"):
    """Update status if callback is set"""
    global _status_callback
    if _status_callback is not None:
        try:
            _status_callback(message, status_type)
        except Exception as e:
            # Silently ignore errors in callback
            pass


def clear_status_callback():
    """Clear the status callback"""
    global _status_callback
    _status_callback = None
