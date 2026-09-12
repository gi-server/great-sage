"""
On-disk trie index for person-centric document storage.

Layout
------
data/people/
└── <person_id>/              ← one directory per known person (UUID)
    ├── profile.json          ← person identity fields (name, dob, created_at)
    ├── trie/
    │   ├── root.json         ← trie root node
    │   └── nodes/
    │       └── <sha256>.json ← intermediate / leaf nodes keyed by hashed prefix
    └── documents/
        └── <job_id>.json     ← full extracted data for each processed document

Trie structure
--------------
Each node is a JSON file:

    {
      "char": "J",
      "children": {"o": "<sha256_of_Jo>", "a": "<sha256_of_Ja>"},
      "person_ids": ["uuid1", "uuid2"]   ← non-empty only on leaf/partial nodes
    }

The trie is keyed on the lower-cased person_name characters.  Searching by
prefix walks child pointers until the query is exhausted, then returns all
`person_ids` accumulated along that path (for prefix-match semantics) or
stored at the final node (for exact-match semantics).

Idempotency
-----------
`insert()` checks `is_job_processed()` before writing.  If the same job_id
has already been committed for this person, the call is a no-op.  This makes
worker retries safe — the trie and document record are never duplicated.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("great_sage.trie_store")

_PEOPLE_DIR = Path("./data/people")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _person_dir(person_id: str) -> Path:
    return _PEOPLE_DIR / person_id


def _trie_root_path(person_id: str) -> Path:
    return _person_dir(person_id) / "trie" / "root.json"


def _trie_nodes_dir(person_id: str) -> Path:
    return _person_dir(person_id) / "trie" / "nodes"


def _node_path(person_id: str, key: str) -> Path:
    """Path to a trie node file, keyed by SHA-256 of the prefix string."""
    h = hashlib.sha256(key.encode()).hexdigest()
    return _trie_nodes_dir(person_id) / f"{h}.json"


def _documents_dir(person_id: str) -> Path:
    return _person_dir(person_id) / "documents"


def _document_path(person_id: str, job_id: str) -> Path:
    return _documents_dir(person_id) / f"{job_id}.json"


def _profile_path(person_id: str) -> Path:
    return _person_dir(person_id) / "profile.json"


# ---------------------------------------------------------------------------
# Low-level JSON I/O (atomic write via tmp → rename)
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> Optional[Dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except Exception:
        logger.exception("Failed to read %s", path)
        return None


def _write_json(path: Path, data: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
        os.replace(tmp, path)  # atomic on Windows and POSIX; overwrites destination
    except Exception:
        logger.exception("Failed to write %s", path)
        raise


# ---------------------------------------------------------------------------
# Trie node operations
# ---------------------------------------------------------------------------

def _load_root(person_id: str) -> Dict:
    root = _read_json(_trie_root_path(person_id))
    if root is None:
        root = {"char": "", "children": {}, "person_ids": []}
    return root


def _save_root(person_id: str, node: Dict) -> None:
    _write_json(_trie_root_path(person_id), node)


def _load_node(person_id: str, prefix: str) -> Dict:
    node = _read_json(_node_path(person_id, prefix))
    if node is None:
        node = {"char": prefix[-1] if prefix else "", "children": {}, "person_ids": []}
    return node


def _save_node(person_id: str, prefix: str, node: Dict) -> None:
    _write_json(_node_path(person_id, prefix), node)


# ---------------------------------------------------------------------------
# Global trie index (across all people, keyed in their own trie dirs)
# We maintain a flat global index for cross-person name lookups.
# ---------------------------------------------------------------------------

_GLOBAL_INDEX_PATH = _PEOPLE_DIR / "_index.json"


def _load_global_index() -> Dict:
    idx = _read_json(_GLOBAL_INDEX_PATH)
    return idx if idx is not None else {}


def _save_global_index(idx: Dict) -> None:
    _PEOPLE_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(_GLOBAL_INDEX_PATH, idx)


def _index_person(person_id: str, person_name: str, dob: Optional[str]) -> None:
    """
    Insert/update the global flat index used for find_person() lookups.

    The index maps (lowercased name, dob) → person_id.
    """
    idx = _load_global_index()
    key = _make_index_key(person_name, dob)
    if key not in idx:
        idx[key] = person_id
        _save_global_index(idx)


def _make_index_key(person_name: str, dob: Optional[str]) -> str:
    name_norm = person_name.strip().lower()
    dob_norm = (dob or "").strip()
    return f"{name_norm}|{dob_norm}"


# ---------------------------------------------------------------------------
# Person trie insert
# ---------------------------------------------------------------------------

def _trie_insert(person_id: str, name_key: str) -> None:
    """
    Insert `person_id` into the trie for `person_id` under key `name_key`.

    Walks character by character from the root, creating nodes as needed,
    and stores person_id at the leaf.
    """
    # Root node
    root = _load_root(person_id)
    if name_key not in root["children"]:
        root["children"][name_key[0] if name_key else ""] = name_key[0] if name_key else ""
    _save_root(person_id, root)

    # Walk prefix-by-prefix
    prefix = ""
    for ch in name_key:
        prefix += ch
        node = _load_node(person_id, prefix)
        next_ch = name_key[len(prefix)] if len(prefix) < len(name_key) else None
        if next_ch and next_ch not in node["children"]:
            node["children"][next_ch] = prefix + next_ch
        if len(prefix) == len(name_key):
            # Leaf — record person_id
            if person_id not in node["person_ids"]:
                node["person_ids"].append(person_id)
        _save_node(person_id, prefix, node)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_job_processed(person_id: str, job_id: str) -> bool:
    """Return True if this job_id has already been committed for this person."""
    return _document_path(person_id, job_id).exists()


def insert(
    person_id: str,
    person_name: str,
    extracted_data: Dict[str, Any],
    job_id: str,
    dob: Optional[str] = None,
) -> None:
    """
    Persist extracted document data for a person and update the trie index.

    Idempotent: if `job_id` has already been written, this is a no-op.

    Args:
        person_id:      UUID string identifying the person.
        person_name:    Canonical name string (used as the trie key).
        extracted_data: The full LLM classification dict for this document.
        job_id:         Great Sage job UUID (used as the document record key).
        dob:            Date of birth string (used in index for dedup).
    """
    if is_job_processed(person_id, job_id):
        logger.debug("Job %s already recorded for person %s — skipping (idempotent)", job_id, person_id)
        return

    # Ensure person directory structure exists
    _documents_dir(person_id).mkdir(parents=True, exist_ok=True)
    _trie_nodes_dir(person_id).mkdir(parents=True, exist_ok=True)

    # Write document record
    doc_record = {
        "job_id": job_id,
        "person_id": person_id,
        "person_name": person_name,
        "extracted_data": extracted_data,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(_document_path(person_id, job_id), doc_record)

    # Upsert profile
    profile = _read_json(_profile_path(person_id)) or {}
    if not profile:
        profile = {
            "person_id": person_id,
            "person_name": person_name,
            "dob": dob,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    profile["updated_at"] = datetime.now(timezone.utc).isoformat()
    _write_json(_profile_path(person_id), profile)

    # Insert into the person's own trie
    name_key = person_name.strip().lower()
    if name_key:
        _trie_insert(person_id, name_key)

    # Update global flat index
    _index_person(person_id, person_name, dob)

    logger.info("Trie updated: person=%s job=%s name=%s", person_id, job_id, person_name)


def find_person(person_name: str, dob: Optional[str] = None) -> Optional[str]:
    """
    Look up an existing person_id by name and optionally dob.

    Returns the person_id string if found, None otherwise.
    Uses the global flat index for O(1) exact-match lookups.
    """
    idx = _load_global_index()
    key = _make_index_key(person_name, dob)
    person_id = idx.get(key)
    if person_id:
        logger.debug("find_person matched '%s' (dob=%s) → %s", person_name, dob, person_id)
    return person_id


def get_or_create_person(person_name: str, dob: Optional[str] = None) -> str:
    """
    Return the person_id for the given name/dob, creating one if not found.

    This is the main entry point for person resolution after LLM extraction.
    """
    existing = find_person(person_name, dob)
    if existing:
        return existing

    new_id = str(uuid.uuid4())
    logger.info(
        "No existing person found for name='%s' dob='%s' — created new person_id=%s",
        person_name, dob, new_id,
    )
    return new_id


def search_by_name_prefix(prefix: str) -> List[Tuple[str, str]]:
    """
    Return (person_id, person_name) pairs whose name starts with `prefix`.

    Performs a linear scan of the global index (suitable for small datasets).
    For large datasets the trie walk can be used instead, but the flat index
    is simpler and sufficient for the current scale.
    """
    prefix_norm = prefix.strip().lower()
    idx = _load_global_index()
    results: List[Tuple[str, str]] = []
    for key, person_id in idx.items():
        name_part = key.split("|")[0]
        if name_part.startswith(prefix_norm):
            # Load the profile to get the canonical display name
            profile = _read_json(_profile_path(person_id))
            display_name = profile.get("person_name", name_part) if profile else name_part
            results.append((person_id, display_name))
    return results


def get_documents(person_id: str) -> List[Dict[str, Any]]:
    """Return all document records for a person, sorted by processed_at."""
    docs_dir = _documents_dir(person_id)
    if not docs_dir.exists():
        return []

    records = []
    for path in docs_dir.glob("*.json"):
        data = _read_json(path)
        if data:
            records.append(data)

    records.sort(key=lambda r: r.get("processed_at", ""))
    return records
