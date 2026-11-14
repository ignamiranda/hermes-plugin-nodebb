"""hermes-plugin-nodebb: a NodeBB platform plugin for Hermes Agent."""

try:
    from .adapter import register
except ImportError:  # loaded as a loose directory, not a package
    from adapter import register

__all__ = ["register"]
