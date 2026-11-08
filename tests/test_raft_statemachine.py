from kvstore.raft.sim import SimCluster
from kvstore.raft.statemachine import KVStateMachine


def cmd(op, key, ts, cid="c1", seq=1, **kw):
    return {"op": op, "key": key, "ts": ts, "client_id": cid, "seq": seq, **kw}


def test_set_get_delete():
    m = KVStateMachine()
    assert m.apply(1, 1, cmd("SET", "a", 0, seq=1, value=1)) == {"ok": True}
    assert m.apply(2, 1, cmd("GET", "a", 0, seq=2)) == {"ok": True, "value": 1}
    assert m.apply(3, 1, cmd("DELETE", "a", 0, seq=3)) == {"ok": True}
    assert m.apply(4, 1, cmd("GET", "a", 0, seq=4))["error"] == "not_found"


def test_retried_command_applies_once():
    m = KVStateMachine()
    m.apply(1, 1, cmd("SET", "a", 0, seq=1, value="first"))
    m.apply(2, 1, cmd("SET", "a", 0, seq=2, value="second"))
    # a retry of seq 2 arrives again (client timed out) and another stale retry of seq 1
    assert m.apply(3, 1, cmd("SET", "a", 0, seq=2, value="second")) == {"ok": True}
    assert m.apply(4, 1, cmd("SET", "a", 0, seq=1, value="first"))["error"] == "stale_seq"
    assert m.apply(5, 1, cmd("GET", "a", 0, cid="other", seq=1))["value"] == "second"


def test_ttl_is_absolute_and_uses_leader_timestamps_only():
    m = KVStateMachine()
    m.apply(1, 1, cmd("SET", "k", 100.0, seq=1, value="v", ttl=10))
    assert m.apply(2, 1, cmd("GET", "k", 109.9, seq=2))["value"] == "v"
    assert m.apply(3, 1, cmd("GET", "k", 110.0, seq=3))["error"] == "not_found"


def test_replicas_applying_same_commands_have_identical_state_regardless_of_wall_clock():
    import time as real_time

    cmds = [cmd("SET", f"k{i}", 50.0 + i, seq=i + 1, value=i, ttl=5 if i % 2 else None) for i in range(10)]
    cmds.append(cmd("GET", "k1", 56.0, seq=11))
    a, b = KVStateMachine(), KVStateMachine()
    for i, c in enumerate(cmds, 1):
        a.apply(i, 1, c)
    real_time.sleep(0.01)  # b runs "later" on its own clock; it must not matter
    for i, c in enumerate(cmds, 1):
        b.apply(i, 1, c)
    assert a.digest() == b.digest()


def test_lru_eviction_still_works_inside_replicated_store():
    m = KVStateMachine(max_size=2)
    for i, k in enumerate("abc", 1):
        m.apply(i, 1, cmd("SET", k, 0, seq=i, value=i))
    assert m.apply(4, 1, cmd("GET", "a", 0, seq=4))["error"] == "not_found"
    assert m.apply(5, 1, cmd("GET", "c", 0, seq=5))["value"] == 3


def test_cluster_replicas_converge_to_same_state_and_dedupe_retries():
    c = SimCluster(3, seed=17, drop=0.1, dup=0.1)
    assert c.run_until(lambda: c.leader() is not None, 5)
    for i in range(1, 9):
        command = cmd("SET", f"k{i % 3}", float(i), seq=i, value=i)
        c.propose(command)
        c.propose(dict(command))  # client retry of the very same (client_id, seq)
        c.run(0.1)
    c.drop = c.dup = 0
    assert c.run_until(c.converged, 10)
    machines = []
    for node in c.up():
        m = KVStateMachine()
        for index, command in c.applied[node.id]:
            m.apply(index, 0, command)
        machines.append(m)
    assert len({m.digest() for m in machines}) == 1
    assert machines[0].sessions["c1"][0] == 8  # last seq applied exactly once
