"""Cluster-level tests on the simulator, including 200-seed fault injection and a mutation check."""

import random
import struct

import pytest

from kvstore.raft import node as node_mod
from kvstore.raft.sim import InvariantViolation, SimCluster
from kvstore.raft.storage import FileStorage


def elected(c, timeout=5.0):
    assert c.run_until(lambda: c.leader() is not None, timeout), f"no leader (seed {c.seed})"
    return c.leader()


def test_at_most_one_leader_per_term_over_long_lossy_run():
    c = SimCluster(5, seed=3, drop=0.2, dup=0.1)
    for step in range(1500):
        if step % 200 == 100 and c.leader():
            c.crash(c.leader().id)
        if step % 200 == 150:
            for i in c.ids:
                c.restart(i)
        c.step()  # invariants (including one leader per term) are checked every step
    assert all(len(v) <= 1 for v in c.leaders_by_term.values())


def test_leader_failover_loses_no_acknowledged_write():
    c = SimCluster(3, seed=11)
    old = elected(c)
    for k in range(5):
        c.propose({"k": k})
        c.run(0.1)
    assert c.run_until(lambda: len(c.acked) >= 5, 3)
    c.crash(old.id)
    assert c.run_until(lambda: c.leader() is not None and c.leader().id != old.id, 5)
    c.propose({"k": "after"})
    c.run(1.0)
    c.restart(old.id)
    assert c.run_until(c.converged, 5)
    c.check_acked_present()
    assert {cmd["k"] for _, _, cmd in c.acked if cmd} >= {0, 1, 2, 3, 4, "after"}


def test_restarted_follower_catches_up():
    c = SimCluster(3, seed=5)
    leader = elected(c)
    follower = next(i for i in c.ids if i != leader.id)
    c.crash(follower)
    for k in range(10):
        c.propose({"k": k})
        c.run(0.05)
    c.run(0.5)
    c.restart(follower)
    assert c.run_until(c.converged, 5)
    assert len(c.nodes[follower].log) >= 11


def scenario_minority_cannot_commit(seed=21):
    c = SimCluster(5, seed=seed)
    leader = elected(c)
    others = [i for i in c.ids if i != leader.id]
    minority = {leader.id, others[0]}
    c.partition([minority, set(others[1:])])
    idx, term = c.propose({"k": "minority"}, node_id=leader.id)
    c.run(2.0)
    stale = c.nodes[leader.id]
    assert stale.commit_index < idx, "minority side committed a write"
    return c, idx, term


def test_minority_partition_cannot_commit_and_logs_converge_after_heal():
    c, idx, term = scenario_minority_cannot_commit()
    assert c.run_until(lambda: any(n.role == "leader" and n.id not in c.group and False or
                                   (n.role == "leader" and c.group[n.id] == 1) for n in c.up()), 5)
    c.propose({"k": "majority"}, node_id=next(n.id for n in c.up() if n.role == "leader" and c.group[n.id] == 1))
    c.run(1.0)
    c.heal()
    assert c.run_until(c.converged, 10)
    assert c.nodes[1].log == c.nodes[2].log == c.nodes[3].log == c.nodes[4].log == c.nodes[5].log
    assert all(entry[1] != {"k": "minority"} for entry in c.nodes[1].log)  # the uncommitted entry was discarded


def test_torn_write_leaves_the_log_consistent(tmp_path):
    c = SimCluster(3, seed=8, storage_factory=lambda i: FileStorage(str(tmp_path / f"n{i}"), fsync=False))
    leader = elected(c)
    for k in range(6):
        c.propose({"k": k})
        c.run(0.1)
    victim = next(i for i in c.ids if i != leader.id)
    c.crash(victim)
    path = c.storages[victim].path
    c.storages[victim].close()
    before = list(c.storages[victim].entries)
    with open(path, "ab") as f:  # crash mid-write of a NEW record: header promises 200 bytes, 7 arrive
        f.write(struct.pack(">II", 200, 0) + b"{\"t\":\"ap")  # never fsynced, never acknowledged
    c.storages[victim] = FileStorage(str(tmp_path / f"n{victim}"), fsync=False)
    assert c.storages[victim].entries == before  # the torn tail is dropped, acknowledged data kept
    c.restart(victim)
    assert c.run_until(c.converged, 8)
    c.check_acked_present()


def run_fuzz_seed(seed: int, steps: int = 500) -> None:
    rng = random.Random(seed)
    c = SimCluster(rng.choice([3, 3, 5]), seed=seed, drop=0.1, dup=0.05)
    counter = 0
    for _ in range(steps):
        r = rng.random()
        if r < 0.25:
            counter += 1
            ids = [n.id for n in c.up() if n.role == "leader"]
            if ids:
                c.propose({"c": counter}, node_id=rng.choice(ids))
        elif r < 0.27 and len(c.up()) > 1:
            c.crash(rng.choice([n.id for n in c.up()]))
        elif r < 0.31:
            for i in c.ids:
                if c.nodes[i] is None and rng.random() < 0.5:
                    c.restart(i)
        elif r < 0.315:
            ids = c.ids[:]
            rng.shuffle(ids)
            cut = rng.randint(1, len(ids) - 1)
            c.partition([set(ids[:cut]), set(ids[cut:])])
        elif r < 0.33:
            c.heal()
        c.step()
    c.heal()
    c.drop = c.dup = 0.0
    for i in c.ids:
        c.restart(i)
    if not c.run_until(c.converged, 15):
        raise InvariantViolation(f"seed={seed}: cluster did not converge after healing")
    c.check_acked_present()


@pytest.mark.parametrize("batch", range(10))
def test_randomized_fault_injection_200_seeds(batch):
    """200 seeds total (20 per batch). The failing seed is in the message; replay with
    `python -c "from tests.test_raft_cluster import run_fuzz_seed; run_fuzz_seed(<seed>)"`."""
    for seed in range(batch * 20, batch * 20 + 20):
        run_fuzz_seed(seed)


def test_mutation_majority_of_one_is_caught(monkeypatch):
    monkeypatch.setattr(node_mod, "quorum", lambda n: 1)
    with pytest.raises(AssertionError):
        scenario_minority_cannot_commit()
    caught = 0
    for seed in range(15):
        try:
            run_fuzz_seed(seed, steps=300)
        except AssertionError:
            caught += 1
    assert caught > 0, "fault injection failed to detect a broken majority rule"
