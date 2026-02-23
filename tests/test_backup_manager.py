"""Tests for BlackRoad Backup Manager."""
import os
import tarfile
import pytest
from backup_manager import (
    create_backup, restore, verify_integrity, list_jobs, get_job,
    prune_old, schedule_info, stats, get_db, BackupJob,
    _sha256_file, _sha256_dir_manifest, _job_id,
)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_backup.db")


@pytest.fixture
def source_dir(tmp_path):
    """Create a source directory with test files."""
    src = tmp_path / "source"
    src.mkdir()
    (src / "file1.txt").write_text("Hello World")
    (src / "file2.txt").write_text("Second file content")
    subdir = src / "subdir"
    subdir.mkdir()
    (subdir / "nested.txt").write_text("Nested content here")
    return str(src)


@pytest.fixture
def dest_dir(tmp_path):
    d = tmp_path / "backups"
    d.mkdir()
    return str(d)


def test_create_full_backup(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    assert job.status == "success"
    assert job.backup_type == "full"
    assert job.file_count == 3
    assert job.size_bytes > 0
    assert len(job.checksum) == 64
    assert os.path.exists(job.dest_path)


def test_create_backup_creates_valid_tar(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    with tarfile.open(job.dest_path, "r:*") as tar:
        members = [m.name for m in tar.getmembers()]
    assert "file1.txt" in members
    assert "file2.txt" in members
    assert "subdir/nested.txt" in members


def test_create_backup_compression_gz(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, compression="gz", db_path=tmp_db)
    assert job.dest_path.endswith(".tar.gz")
    assert os.path.exists(job.dest_path)


def test_create_backup_compression_bz2(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, compression="bz2", db_path=tmp_db)
    assert job.dest_path.endswith(".tar.bz2")


def test_create_backup_no_compression(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, compression="none", db_path=tmp_db)
    assert job.dest_path.endswith(".tar")


def test_restore_backup(source_dir, dest_dir, tmp_path, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    restore_dir = str(tmp_path / "restored")
    result = restore(job.id, restore_dir, db_path=tmp_db)
    assert result["status"] == "success"
    assert result["files_written"] == 3
    assert os.path.exists(os.path.join(restore_dir, "file1.txt"))
    assert os.path.exists(os.path.join(restore_dir, "file2.txt"))
    assert os.path.exists(os.path.join(restore_dir, "subdir", "nested.txt"))


def test_restore_content_integrity(source_dir, dest_dir, tmp_path, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    restore_dir = str(tmp_path / "restored")
    restore(job.id, restore_dir, db_path=tmp_db)
    assert open(os.path.join(restore_dir, "file1.txt")).read() == "Hello World"
    assert open(os.path.join(restore_dir, "file2.txt")).read() == "Second file content"


def test_restore_missing_job(dest_dir, tmp_db):
    with pytest.raises(ValueError):
        restore("nonexistent_job", dest_dir, db_path=tmp_db)


def test_verify_integrity_valid(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    result = verify_integrity(job.id, db_path=tmp_db)
    assert result["valid"] is True
    assert result["checksum_ok"] is True
    assert result["tar_readable"] is True
    assert result["member_count"] == 3


def test_verify_integrity_missing_archive(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    os.unlink(job.dest_path)
    result = verify_integrity(job.id, db_path=tmp_db)
    assert result["valid"] is False


def test_verify_integrity_corrupted(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    # Corrupt the archive
    with open(job.dest_path, "wb") as fh:
        fh.write(b"CORRUPTED DATA")
    result = verify_integrity(job.id, db_path=tmp_db)
    assert result["valid"] is False
    assert result["checksum_ok"] is False


def test_verify_integrity_missing_job(tmp_db):
    with pytest.raises(ValueError):
        verify_integrity("nonexistent", db_path=tmp_db)


def test_incremental_backup(source_dir, dest_dir, tmp_db):
    full_job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    assert full_job.status == "success"

    # Add a new file
    os.makedirs(source_dir, exist_ok=True)
    with open(os.path.join(source_dir, "newfile.txt"), "w") as fh:
        fh.write("New file added after full backup")

    inc_job = create_backup(source_dir, dest_dir, backup_type="incremental", db_path=tmp_db)
    assert inc_job.status == "success"
    assert inc_job.parent_job_id in (full_job.id, "")


def test_list_jobs_empty(tmp_db):
    assert list_jobs(db_path=tmp_db) == []


def test_list_jobs(source_dir, dest_dir, tmp_db):
    create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    jobs = list_jobs(db_path=tmp_db)
    assert len(jobs) == 2


def test_list_jobs_by_type(source_dir, dest_dir, tmp_db):
    create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    jobs = list_jobs(backup_type="full", db_path=tmp_db)
    assert len(jobs) == 1
    assert jobs[0].backup_type == "full"


def test_get_job(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    fetched = get_job(job.id, db_path=tmp_db)
    assert fetched is not None
    assert fetched.id == job.id


def test_get_job_missing(tmp_db):
    assert get_job("nonexistent", db_path=tmp_db) is None


def test_prune_old(source_dir, dest_dir, tmp_db):
    jobs = []
    for _ in range(7):
        j = create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
        jobs.append(j)
    result = prune_old(source_dir, keep_last=5, db_path=tmp_db)
    assert len(result["deleted"]) == 2
    remaining = list_jobs(backup_type="full", db_path=tmp_db)
    assert len(remaining) == 5


def test_prune_old_dry_run(source_dir, dest_dir, tmp_db):
    for _ in range(4):
        create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    result = prune_old(source_dir, keep_last=2, dry_run=True, db_path=tmp_db)
    assert result["dry_run"] is True
    assert len(result["deleted"]) == 2
    # Files should still exist
    remaining = list_jobs(backup_type="full", db_path=tmp_db)
    assert len(remaining) == 4


def test_prune_does_not_exceed_keep(source_dir, dest_dir, tmp_db):
    for _ in range(3):
        create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    result = prune_old(source_dir, keep_last=10, db_path=tmp_db)
    assert result["deleted"] == []


def test_schedule_info_empty(tmp_db):
    info = schedule_info("/nonexistent/source", db_path=tmp_db)
    assert info["last_full"] is None
    assert "recommendation" in info


def test_schedule_info_with_jobs(source_dir, dest_dir, tmp_db):
    create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    info = schedule_info(source_dir, db_path=tmp_db)
    assert info["last_full"] is not None
    assert info["total_full"] == 1
    assert "recommendation" in info


def test_stats_empty(tmp_db):
    s = stats(db_path=tmp_db)
    assert s["total_jobs"] == 0
    assert s["total_size_bytes"] == 0


def test_stats_populated(source_dir, dest_dir, tmp_db):
    create_backup(source_dir, dest_dir, backup_type="full", db_path=tmp_db)
    create_backup(source_dir, dest_dir, backup_type="incremental", db_path=tmp_db)
    s = stats(db_path=tmp_db)
    assert s["total_jobs"] == 2
    assert s["total_size_bytes"] > 0


def test_sha256_dir_manifest(tmp_path):
    (tmp_path / "a.txt").write_text("content a")
    (tmp_path / "b.txt").write_text("content b")
    m = _sha256_dir_manifest(str(tmp_path))
    assert "a.txt" in m
    assert "b.txt" in m
    assert len(m["a.txt"]) == 64


def test_sha256_file(tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("hello")
    h1 = _sha256_file(str(f))
    assert len(h1) == 64
    f.write_text("hello")
    h2 = _sha256_file(str(f))
    assert h1 == h2


def test_backup_missing_source(dest_dir, tmp_db):
    with pytest.raises(FileNotFoundError):
        create_backup("/nonexistent/path", dest_dir, db_path=tmp_db)


def test_backup_invalid_type(source_dir, dest_dir, tmp_db):
    with pytest.raises(ValueError):
        create_backup(source_dir, dest_dir, backup_type="invalid", db_path=tmp_db)


def test_db_schema(tmp_path):
    db_path = str(tmp_path / "fresh.db")
    conn = get_db(db_path)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "backup_jobs" in tables
    assert "file_manifests" in tables
    assert "restore_events" in tables


def test_backup_with_tags(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, tags=["prod", "weekly"], db_path=tmp_db)
    assert "prod" in job.tags
    assert job.status == "success"


def test_backup_with_exclude_patterns(source_dir, dest_dir, tmp_db):
    job = create_backup(source_dir, dest_dir, exclude_patterns=["*.txt"], db_path=tmp_db)
    assert job.status == "success"
    # All .txt files excluded → 0 files
    assert job.file_count == 0
