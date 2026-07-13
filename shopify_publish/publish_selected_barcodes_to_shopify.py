#!/usr/bin/env python3
"""
Legacy path for Mac ad-hoc publishes with ``shopify_publish/.env.prod``.

Delegates to ``scripts/publish/publish_selected_barcodes_to_shopify.py`` (canonical service).
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.publish.publish_selected_barcodes_to_shopify import main

if __name__ == "__main__":
    main()
