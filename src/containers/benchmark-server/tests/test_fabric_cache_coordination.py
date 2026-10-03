import sys
import unittest
from pathlib import Path
from unittest.mock import patch


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.engine_runner import (  # noqa: E402
    _post_fabric_cache_with_retry,
)


class FabricCacheCoordinationTest(unittest.TestCase):
    @patch("services.engine_runner._post_with_retry")
    def test_shared_cache_post_waits_for_lock(self, post):
        inside_lock = False

        class Guard:
            def __enter__(self):
                nonlocal inside_lock
                inside_lock = True

            def __exit__(self, *_args):
                nonlocal inside_lock
                inside_lock = False

        def assert_locked(*_args):
            self.assertTrue(inside_lock)

        post.side_effect = assert_locked
        with patch("services.engine_runner._FABRIC_CACHE_LOCK", Guard()):
            _post_fabric_cache_with_retry("url", lambda _message: None, "fabric")

        post.assert_called_once()


if __name__ == "__main__":
    unittest.main()
