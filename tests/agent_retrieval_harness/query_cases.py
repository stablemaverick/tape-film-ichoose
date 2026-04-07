"""
Agent-style user queries -> expected catalog fixture id.

Pass = expected row appears within top_k ranked results (see harness config).

``TOP1_EVAL_CASES`` — Tape Agent v1 expected top-1 set (ranking workstream); 15 queries.
"""

from __future__ import annotations

from typing import List, Tuple

# Expected top-1 evaluation set (no ambiguous franchise-only queries).
TOP1_EVAL_CASES: List[Tuple[str, str]] = [
    ("ghost in the shell sac 2045", "ghost_sac_2045"),
    ("ghost in shell part 2", "ghost_sac_2045"),
    ("ajin", "ajin_s2"),
    ("father brown", "father_brown_s10"),
    ("prison break season 3", "prison_break_s3"),
    ("wombles complete series", "wombles_complete"),
    ("fleabag", "fleabag_s1"),
    ("otaku elf", "otaku_elf"),
    ("shirobako", "shirobako_le"),
    ("alex de la iglesia", "alex_de_la_iglesia_3films"),
    ("sophia loren boxset", "sophia_loren_box"),
    ("yokai monsters", "yokai_monsters"),
    ("takashi ishii", "takashi_ishii"),
    ("hanasaku iroha", "hanasaku_collection"),
    ("monthly girls nozaki kun", "nozaki_kun"),
]

# Backwards-compatible alias for generic retrieval harness.
QUERY_CASES: List[Tuple[str, str]] = list(TOP1_EVAL_CASES)

# Optional: franchise ambiguity (baseline often prefers year-marked film SKU).
OPTIONAL_STRESS_CASES: List[Tuple[str, str]] = [
    ("ghost in the shell", "ghost_sac_2045"),
]
