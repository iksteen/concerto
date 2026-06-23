from concerto.board import aggregate_status_counts as _aggregate_status_counts


def test_aggregate_status_counts() -> None:
    # +1 outranks ?, which outranks pray; each user counted once.
    reactions = [
        {"name": "thumbsup", "users": ["U1", "U2"]},
        {"name": "ticket", "users": ["U2"]},  # U2 already counted as going
        {"name": "eyes", "users": ["U2", "U3"]},  # U2 has a ticket -> not undecided
        {"name": "pray", "users": ["U3", "U4"]},  # U3 is undecided -> not looking
    ]
    assert _aggregate_status_counts(reactions) == (2, 1, 1)

    # Ignored emoji and malformed entries contribute nothing.
    assert _aggregate_status_counts([{"name": "heart", "users": ["U1"]}]) == (0, 0, 0)
    assert _aggregate_status_counts(None) == (0, 0, 0)
    assert _aggregate_status_counts([{"name": "pray"}, "junk"]) == (0, 0, 0)


if __name__ == "__main__":
    test_aggregate_status_counts()
    print("ok")
