from kvstore.raft.linearizability import INF, Op, check_history, is_linearizable


def test_sequential_history_is_linearizable():
    ops = [Op("w", 1, 0, 1), Op("r", 1, 2, 3), Op("w", 2, 4, 5), Op("r", 2, 6, 7)]
    assert is_linearizable(ops)


def test_stale_read_after_completed_write_is_rejected():
    ops = [Op("w", 1, 0, 1), Op("w", 2, 2, 3), Op("r", 1, 4, 5)]  # read after w2 finished returns old value
    assert not is_linearizable(ops)


def test_read_of_never_written_value_is_rejected():
    assert not is_linearizable([Op("w", 1, 0, 1), Op("r", 99, 2, 3)])


def test_concurrent_write_and_read_may_see_either_value():
    assert is_linearizable([Op("w", 1, 0, 10), Op("r", None, 1, 2)])
    assert is_linearizable([Op("w", 1, 0, 10), Op("r", 1, 1, 2)])


def test_reads_cannot_go_back_in_time():
    ops = [Op("w", 1, 0, 100), Op("r", 1, 1, 2), Op("r", None, 3, 4)]  # saw 1, then saw "absent" again
    assert not is_linearizable(ops)


def test_unknown_outcome_write_may_have_happened_or_not():
    assert is_linearizable([Op("w", 5, 0, INF, maybe=True), Op("r", 5, 10, 11)])
    assert is_linearizable([Op("w", 5, 0, INF, maybe=True), Op("r", None, 10, 11)])


def test_check_history_names_the_bad_key():
    ok, key = check_history({"a": [Op("w", 1, 0, 1), Op("r", 1, 2, 3)],
                             "b": [Op("w", 1, 0, 1), Op("w", 2, 2, 3), Op("r", 1, 4, 5)]})
    assert not ok and key == "b"
