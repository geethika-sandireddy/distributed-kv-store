"""Rule-level tests for RaftNode (Figure 2). Messages are fed by hand; no network."""

import random

from kvstore.raft.node import CANDIDATE, FOLLOWER, LEADER, Config, RaftNode
from kvstore.raft.storage import MemoryStorage


def mk(node_id=1, peers=(1, 2, 3), entries=(), term=0, vote=None, seed=0):
    st = MemoryStorage()
    st.entries = list(entries)
    st.term, st.voted_for = term, vote
    applied = []
    n = RaftNode(node_id, list(peers), st, random.Random(seed),
                 apply_fn=lambda i, t, c: applied.append((i, c)))
    return n, st, applied


def vote_req(term, cand, last_index=0, last_term=0):
    return {"type": "request_vote", "term": term, "from": cand,
            "last_log_index": last_index, "last_log_term": last_term}


def append(term, leader, prev_index, prev_term, entries, commit=0):
    return {"type": "append", "term": term, "from": leader, "prev_index": prev_index,
            "prev_term": prev_term, "entries": [list(e) for e in entries], "leader_commit": commit}


def reply(out):
    assert len(out) == 1
    return out[0][1]


def elect(n):
    """Drive a node to leader by timing out and granting a vote from peer 2."""
    n.tick(10.0)
    assert n.role == CANDIDATE
    n.handle({"type": "vote_reply", "term": n.term, "granted": True, "from": 2}, 10.0)
    assert n.role == LEADER


# ---- voting ----
def test_vote_rejected_for_old_term():
    n, *_ = mk(term=5)
    assert reply(n.handle(vote_req(4, 2), 0))["granted"] is False


def test_vote_granted_once_per_term():
    n, st, _ = mk()
    assert reply(n.handle(vote_req(1, 2), 0))["granted"] is True
    assert st.voted_for == 2 and st.term == 1  # persisted before the reply was built
    assert reply(n.handle(vote_req(1, 3), 0))["granted"] is False
    assert reply(n.handle(vote_req(1, 2), 0))["granted"] is True  # same candidate again is fine


def test_vote_requires_candidate_log_at_least_as_up_to_date():
    log = [(1, "a"), (2, "b")]
    n, *_ = mk(entries=log)
    assert reply(n.handle(vote_req(3, 2, last_index=5, last_term=1), 0))["granted"] is False  # older last term
    n, *_ = mk(entries=log)
    assert reply(n.handle(vote_req(3, 2, last_index=1, last_term=2), 0))["granted"] is False  # same term, shorter
    n, *_ = mk(entries=log)
    assert reply(n.handle(vote_req(3, 2, last_index=2, last_term=2), 0))["granted"] is True
    n, *_ = mk(entries=log)
    assert reply(n.handle(vote_req(3, 2, last_index=1, last_term=3), 0))["granted"] is True  # higher last term wins


def test_higher_term_makes_leader_step_down():
    n, *_ = mk()
    elect(n)
    n.handle(append(n.term + 1, 3, 0, 0, []), 11.0)
    assert n.role == FOLLOWER and n.leader_id == 3


# ---- append entries ----
def test_append_rejected_when_previous_entry_missing_or_mismatched():
    n, *_ = mk(entries=[(1, "a")])
    assert reply(n.handle(append(1, 2, 3, 1, [(1, "x")]), 0))["success"] is False
    assert reply(n.handle(append(1, 2, 1, 2, [(2, "x")]), 0))["success"] is False


def test_conflicting_entries_and_everything_after_are_deleted():
    n, st, _ = mk(entries=[(1, "a"), (1, "b"), (1, "c")])
    r = reply(n.handle(append(2, 2, 1, 1, [(2, "B")]), 0))
    assert r["success"] and n.log == [(1, "a"), (2, "B")] and st.entries == n.log


def test_old_or_duplicate_append_does_not_truncate_matching_suffix():
    n, *_ = mk(entries=[(1, "a"), (1, "b"), (1, "c")])
    n.handle(append(1, 2, 0, 0, [(1, "a")]), 0)  # delayed message carrying an old prefix
    assert n.log == [(1, "a"), (1, "b"), (1, "c")]


def test_commit_is_min_of_leader_commit_and_last_new_entry():
    n, _, applied = mk()
    n.handle(append(1, 2, 0, 0, [(1, "a"), (1, "b")], commit=9), 0)
    assert n.commit_index == 2 and applied == [(1, "a"), (2, "b")]


def test_committed_entries_are_applied_in_order_once():
    n, _, applied = mk()
    n.handle(append(1, 2, 0, 0, [(1, "a"), (1, "b")], commit=1), 0)
    n.handle(append(1, 2, 2, 1, [(1, "c")], commit=3), 0)
    n.handle(append(1, 2, 2, 1, [(1, "c")], commit=3), 0)
    assert [c for _, c in applied] == ["a", "b", "c"]


# ---- leader behaviour ----
def test_new_leader_appends_noop_in_its_own_term():
    n, st, _ = mk()
    elect(n)
    assert n.log[-1] == (n.term, None) and st.entries[-1] == (n.term, None)


def test_figure8_leader_does_not_commit_old_term_entry_by_counting_replicas():
    # Node 1 holds an entry from term 1 and becomes leader in a later term.
    n, *_ = mk(entries=[(1, "old")], term=1)
    elect(n)  # term 2; log = [(1,old), (2,noop)]
    # Peer 2 acknowledges only the old-term entry: majority holds index 1, but it is not from term 2.
    n.handle({"type": "append_reply", "term": n.term, "success": True, "match_index": 1, "from": 2}, 11.0)
    assert n.commit_index == 0
    # Once the current-term entry is replicated on a majority, both commit.
    n.handle({"type": "append_reply", "term": n.term, "success": True, "match_index": 2, "from": 2}, 11.0)
    assert n.commit_index == 2


def test_failed_append_reply_backs_up_next_index():
    n, *_ = mk(entries=[(1, "a"), (1, "b")], term=1)
    elect(n)
    n.next_index[2] = 3
    n.handle({"type": "append_reply", "term": n.term, "success": False, "match_index": 0,
              "conflict_index": 1, "from": 2}, 11.0)
    assert n.next_index[2] == 1


def test_follower_proposal_is_refused():
    n, *_ = mk()
    assert n.propose({"x": 1}, 0.0) == (None, [])


def test_single_node_cluster_elects_itself_and_commits():
    n, _, applied = mk(peers=(1,))
    n.tick(10.0)
    assert n.role == LEADER
    idx, _ = n.propose("cmd", 10.0)
    assert n.commit_index == idx and applied == [(idx, "cmd")]


# ---- timing ----
def test_election_timeouts_are_randomized_and_heartbeat_is_much_shorter():
    deadlines = {mk(seed=s)[0]._election_deadline for s in range(20)}
    assert len(deadlines) > 10
    cfg = Config()
    assert cfg.heartbeat_interval * 2 < cfg.election_timeout[0]
