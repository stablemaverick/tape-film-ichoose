#!/usr/bin/env python3
"""
Legacy entrypoint: delegates to ``app.services.catalog_shopify_publish_service`` and
``jobs.publish_catalog_to_shopify``. Prefer::

    python -m jobs.publish_catalog_to_shopify --help
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from jobs.publish_catalog_to_shopify import main

if __name__ == "__main__":
    raise SystemExit(main())
