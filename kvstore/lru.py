"""
LRU eviction tracking.

A doubly linked list of keys, most-recently-used at the head, combined
with a dict from key -> node for O(1) lookup. This is deliberately kept
separate from Store: it only tracks *order*, not values, so Store stays
the single source of truth for data and this stays easy to unit test on
its own.
"""



class _Node:
    __slots__ = ("key", "prev", "next")

    def __init__(self, key: str):
        self.key = key
        self.prev: _Node | None = None
        self.next: _Node | None = None


class LRUTracker:
    """Tracks access order for a fixed-capacity store.

    touch(key) marks a key as most-recently-used (inserting it if new).
    remove(key) drops a key from tracking (e.g. on explicit delete).
    evict_candidate() returns the least-recently-used key without
    removing it -- the caller removes it from the store first, then
    calls remove() here, keeping the two structures consistent even if
    the store-side delete fails.
    """

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._nodes: dict[str, _Node] = {}
        self._head: _Node | None = None  # most recently used
        self._tail: _Node | None = None  # least recently used

    def __len__(self) -> int:
        return len(self._nodes)

    def touch(self, key: str) -> None:
        if key in self._nodes:
            self._unlink(self._nodes[key])
        node = self._nodes.get(key) or _Node(key)
        self._nodes[key] = node
        self._push_front(node)

    def remove(self, key: str) -> None:
        node = self._nodes.pop(key, None)
        if node is not None:
            self._unlink(node)

    def is_over_capacity(self) -> bool:
        return len(self._nodes) > self.capacity

    def evict_candidate(self) -> str | None:
        """Returns the LRU key, or None if empty. Does not remove it."""
        return self._tail.key if self._tail is not None else None

    def _push_front(self, node: _Node) -> None:
        node.prev = None
        node.next = self._head
        if self._head is not None:
            self._head.prev = node
        self._head = node
        if self._tail is None:
            self._tail = node

    def _unlink(self, node: _Node) -> None:
        if node.prev is not None:
            node.prev.next = node.next
        else:
            self._head = node.next
        if node.next is not None:
            node.next.prev = node.prev
        else:
            self._tail = node.prev
        node.prev = node.next = None
