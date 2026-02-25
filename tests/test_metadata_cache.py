from engine import metadata


def test_metadata_file_sets_are_cached_across_classify_calls() -> None:
    metadata._load_set.cache_clear()

    for _ in range(10):
        out = metadata.classify("info", "gmail.com")
        assert {"is_disposable", "is_role", "is_free"} <= set(out.keys())

    cache_info = metadata._load_set.cache_info()
    assert cache_info.misses == 3
