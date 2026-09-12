"""
Unit tests for app.trie_store.

Tests cover:
- insert writes a document record
- insert is idempotent (same job_id twice = no duplicate)
- find_person returns None for unknown person
- find_person matches on name+dob
- find_person matches name-only when dob is None
- get_or_create_person creates a new UUID when not found
- get_or_create_person returns existing person_id on second call
- search_by_name_prefix returns matches
- get_documents returns all documents for a person
- is_job_processed returns correct True/False
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Fixtures: redirect trie_store's _PEOPLE_DIR to a temp path
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_people_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect all trie_store filesystem paths to a temp directory."""
    import app.trie_store as ts

    people_dir = tmp_path / "people"
    people_dir.mkdir()

    monkeypatch.setattr(ts, "_PEOPLE_DIR", people_dir)
    monkeypatch.setattr(ts, "_GLOBAL_INDEX_PATH", people_dir / "_index.json")

    # Re-patch the helper functions that close over the module-level constant
    monkeypatch.setattr(ts, "_person_dir", lambda pid: people_dir / pid)
    monkeypatch.setattr(ts, "_trie_root_path", lambda pid: people_dir / pid / "trie" / "root.json")
    monkeypatch.setattr(ts, "_trie_nodes_dir", lambda pid: people_dir / pid / "trie" / "nodes")
    monkeypatch.setattr(ts, "_node_path", lambda pid, key: (
        people_dir / pid / "trie" / "nodes" /
        f"{__import__('hashlib').sha256(key.encode()).hexdigest()}.json"
    ))
    monkeypatch.setattr(ts, "_documents_dir", lambda pid: people_dir / pid / "documents")
    monkeypatch.setattr(ts, "_document_path", lambda pid, jid: people_dir / pid / "documents" / f"{jid}.json")
    monkeypatch.setattr(ts, "_profile_path", lambda pid: people_dir / pid / "profile.json")


# ---------------------------------------------------------------------------
# Tests: is_job_processed
# ---------------------------------------------------------------------------

def test_is_job_processed_returns_false_for_new_job() -> None:
    import app.trie_store as ts
    assert ts.is_job_processed("person-1", "job-abc") is False


def test_is_job_processed_returns_true_after_insert() -> None:
    import app.trie_store as ts
    ts.insert("person-1", "Alice Smith", {"document_type": "passport"}, "job-abc", dob="1990-01-01")
    assert ts.is_job_processed("person-1", "job-abc") is True


# ---------------------------------------------------------------------------
# Tests: insert
# ---------------------------------------------------------------------------

def test_insert_writes_document_record(tmp_path: Path) -> None:
    import app.trie_store as ts

    ts.insert("p1", "Bob Jones", {"document_type": "invoice"}, "job-001", dob="1985-06-15")

    doc_path = ts._document_path("p1", "job-001")
    assert doc_path.exists()
    data = json.loads(doc_path.read_text(encoding="utf-8"))
    assert data["job_id"] == "job-001"
    assert data["person_id"] == "p1"
    assert data["person_name"] == "Bob Jones"
    assert data["extracted_data"]["document_type"] == "invoice"


def test_insert_writes_profile() -> None:
    import app.trie_store as ts

    ts.insert("p2", "Carol White", {}, "job-002", dob="1970-03-20")

    profile = json.loads(ts._profile_path("p2").read_text(encoding="utf-8"))
    assert profile["person_id"] == "p2"
    assert profile["person_name"] == "Carol White"
    assert profile["dob"] == "1970-03-20"


def test_insert_is_idempotent() -> None:
    import app.trie_store as ts

    ts.insert("p3", "Dan Brown", {"field": "v1"}, "job-003")
    ts.insert("p3", "Dan Brown", {"field": "v2"}, "job-003")  # same job_id

    # Document file must NOT be overwritten
    data = json.loads(ts._document_path("p3", "job-003").read_text(encoding="utf-8"))
    assert data["extracted_data"]["field"] == "v1"


def test_insert_different_jobs_both_stored() -> None:
    import app.trie_store as ts

    ts.insert("p4", "Eve Black", {}, "job-004a")
    ts.insert("p4", "Eve Black", {}, "job-004b")

    assert ts._document_path("p4", "job-004a").exists()
    assert ts._document_path("p4", "job-004b").exists()


# ---------------------------------------------------------------------------
# Tests: find_person / get_or_create_person
# ---------------------------------------------------------------------------

def test_find_person_returns_none_for_unknown() -> None:
    import app.trie_store as ts
    assert ts.find_person("Nobody Here", "1900-01-01") is None


def test_find_person_matches_name_and_dob() -> None:
    import app.trie_store as ts

    ts.insert("person-x", "Frank Castle", {}, "job-fx", dob="1975-11-22")
    result = ts.find_person("Frank Castle", "1975-11-22")
    assert result == "person-x"


def test_find_person_matches_without_dob() -> None:
    import app.trie_store as ts

    ts.insert("person-y", "Grace Hopper", {}, "job-gy", dob=None)
    result = ts.find_person("Grace Hopper", None)
    assert result == "person-y"


def test_find_person_case_insensitive_key() -> None:
    import app.trie_store as ts

    ts.insert("person-z", "Hank Aaron", {}, "job-hz", dob="1934-02-05")
    # Index key is lowercased — lookup must be case-insensitive too
    result = ts.find_person("hank aaron", "1934-02-05")
    assert result == "person-z"


def test_get_or_create_person_creates_new_uuid() -> None:
    import app.trie_store as ts

    pid = ts.get_or_create_person("Ivy League", "2000-12-01")
    assert pid  # non-empty
    # Calling again returns the SAME id (no person record was written, but
    # the function should return a consistent UUID for this name+dob pair
    # only after at least one insert() has happened to register it)
    # Since no insert was done, a second call creates a DIFFERENT uuid:
    pid2 = ts.get_or_create_person("Ivy League", "2000-12-01")
    # Both are valid UUIDs
    import uuid
    uuid.UUID(pid)
    uuid.UUID(pid2)


def test_get_or_create_person_returns_existing_after_insert() -> None:
    import app.trie_store as ts

    pid = "person-existing"
    ts.insert(pid, "Jack Ryan", {}, "job-jr", dob="1950-07-04")

    result = ts.get_or_create_person("Jack Ryan", "1950-07-04")
    assert result == pid


# ---------------------------------------------------------------------------
# Tests: search_by_name_prefix
# ---------------------------------------------------------------------------

def test_search_by_name_prefix_returns_matches() -> None:
    import app.trie_store as ts

    ts.insert("p-alice", "Alice Cooper", {}, "job-ac", dob=None)
    ts.insert("p-alan", "Alan Turing", {}, "job-at", dob=None)
    ts.insert("p-bob", "Bob Hope", {}, "job-bh", dob=None)

    results = ts.search_by_name_prefix("al")
    ids = [r[0] for r in results]
    assert "p-alice" in ids
    assert "p-alan" in ids
    assert "p-bob" not in ids


def test_search_by_name_prefix_empty_returns_all() -> None:
    import app.trie_store as ts

    ts.insert("p1", "Alice", {}, "j1", dob=None)
    ts.insert("p2", "Bob", {}, "j2", dob=None)

    results = ts.search_by_name_prefix("")
    assert len(results) >= 2


# ---------------------------------------------------------------------------
# Tests: get_documents
# ---------------------------------------------------------------------------

def test_get_documents_returns_all_for_person() -> None:
    import app.trie_store as ts

    ts.insert("p-multi", "MultiDoc Person", {"doc": "a"}, "job-ma", dob=None)
    ts.insert("p-multi", "MultiDoc Person", {"doc": "b"}, "job-mb", dob=None)

    docs = ts.get_documents("p-multi")
    job_ids = [d["job_id"] for d in docs]
    assert "job-ma" in job_ids
    assert "job-mb" in job_ids


def test_get_documents_empty_for_unknown_person() -> None:
    import app.trie_store as ts
    assert ts.get_documents("nonexistent-person") == []
