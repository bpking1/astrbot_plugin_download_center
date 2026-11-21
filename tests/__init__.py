"""Test package setup: ensure AstrBot SDK and plugin package are importable."""

import importlib.util
import os
from pathlib import Path
import sys

if os.path.isdir("/AstrBot") and "/AstrBot" not in sys.path:
    sys.path.insert(0, "/AstrBot")

PLUGIN_ROOT = Path(__file__).resolve().parent.parent
PKG_NAME = "astrbot_plugin_download_center"

if PKG_NAME not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        PKG_NAME,
        PLUGIN_ROOT / "__init__.py",
        submodule_search_locations=[str(PLUGIN_ROOT)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[PKG_NAME] = module
    spec.loader.exec_module(module)
