"""hermes-plugin-nodebb: a NodeBB platform plugin for Hermes Agent."""

register = None
try:
    from .adapter import register
except Exception:  # not loaded as a package (or gateway unavailable)
    try:
        from adapter import register
    except Exception:
        register = None

__all__ = ["register"]
