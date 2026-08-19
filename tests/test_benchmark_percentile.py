import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from benchmarks.benchmark import percentile


def test_p50_of_odd_length_list():
    assert percentile([1, 2, 3], 50) == 2


def test_p0_is_minimum():
    assert percentile([5, 1, 3], 0) == 1


def test_p100_is_maximum():
    assert percentile([5, 1, 3], 100) == 5


def test_unsorted_input_handled():
    assert percentile([10, 2, 8, 4, 6], 50) == 6
