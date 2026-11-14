import sys
from pathlib import Path

# The plugin modules live at the repo root; the fake lives beside the tests.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
