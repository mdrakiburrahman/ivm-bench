import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import Mock, patch

import requests


spec = importlib.util.spec_from_file_location(
    "imds_relay_router",
    Path(__file__).resolve().parents[2] / "imds-router" / "imds_relay_router.py",
)
router = importlib.util.module_from_spec(spec)
spec.loader.exec_module(router)


class RelayTokenTests(unittest.TestCase):
    def test_health_requires_relay_configuration_without_exposing_secrets(self):
        for url, key, status in [('', '', 503), ('https://relay.invalid', '', 503),
                                 ('', 'secret', 503), ('https://relay.invalid', 'secret', 200)]:
            with self.subTest(configured=status==200), patch.object(router,'RELAY_URL',url), \
                 patch.object(router,'RELAY_SENDER_KEY',key):
                handler=Mock(path='/healthz')
                router.Handler.do_GET(handler)
                actual,body=handler._json.call_args.args
                self.assertEqual(actual,status)
                self.assertEqual(body['Healthy'],status==200)
                self.assertNotIn('secret',json.dumps(body))

    def setUp(self):
        self.cache = patch.object(router, "_CACHE", router.TokenCache(300))
        self.cache.start()
        self.addCleanup(self.cache.stop)

    def test_real_http_server_errors_recover_and_warm_cache_avoids_relay(self):
        calls = []
        class Relay(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def do_GET(self):
                calls.append(self.path)
                status = 500 if len(calls) < 4 else 200
                payload = json.dumps({
                    "access_token": "test-token",
                    "expires_on": int(time.time()) + 3600,
                }).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Relay)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        session = requests.Session()
        session.trust_env = False
        try:
            with patch.object(router, "RELAY_URL", f"http://127.0.0.1:{server.server_port}/token"), \
                    patch.object(router, "_relay_session", return_value=session), \
                    patch.object(router.random, "uniform", return_value=0):
                token, error = router._fetch_token("a", None, "a")
                cached, _ = router._fetch_token("a", None, "a")
            self.assertEqual(error, "")
            self.assertEqual(token, cached)
            self.assertEqual(len(calls), 4)
        finally:
            session.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_connect_loss_and_server_errors_recover_on_tenth_attempt(self):
        response = requests.Response()
        response.status_code = 500
        failure = requests.HTTPError(response=response)
        token = {"access_token": "test-token", "expires_on": 10000}
        with patch.object(router, "_relay_fetch", side_effect=[
            requests.ConnectTimeout(), requests.ConnectionError(), failure,
            requests.ReadTimeout(), failure, failure, failure, failure, failure,
            (token, ""),
        ]) as fetch, patch.object(router.time, "sleep"), patch.object(router.time, "monotonic", return_value=0):
            body, error = router._relay_retry("resource", None, 50, lambda *args: None)
        self.assertEqual(body, token)
        self.assertEqual(error, "")
        self.assertEqual(fetch.call_count, 10)

    def test_nontransient_failure_is_not_retried_or_exposed(self):
        response = requests.Response()
        response.status_code = 403
        with patch.object(router, "_relay_fetch", side_effect=requests.HTTPError(
            "private response content", response=response,
        )) as fetch:
            body, error = router._relay_retry(
                "resource", None, router.time.monotonic() + 50, lambda *args: None,
            )
        self.assertIsNone(body)
        self.assertEqual(error, "Relay HTTP 403")
        self.assertEqual(fetch.call_count, 1)

    def test_deadline_stops_retries_and_bounds_backoff(self):
        with patch.object(router, "_relay_fetch", side_effect=requests.ConnectTimeout()) as fetch, \
                patch.object(router.time, "monotonic", side_effect=[0, 49.9, 50.1]), \
                patch.object(router.random, "uniform", return_value=2), \
                patch.object(router.time, "sleep") as sleep:
            body, error = router._relay_retry("resource", None, 50, lambda *args: None)
        self.assertIsNone(body)
        self.assertEqual(error, "Relay ConnectTimeout")
        self.assertEqual(fetch.call_count, 1)
        self.assertAlmostEqual(sleep.call_args.args[0], 0.1)

    def test_cache_separates_scopes_and_refreshes_before_expiry(self):
        now = [1000]
        cache = router.TokenCache(300, now=lambda: now[0])
        def mint(resource, client_id, deadline, log):
            return {"access_token": f"{resource}-{client_id}", "expires_on": now[0] + 1000}, ""
        with patch.object(router, "_CACHE", cache), patch.object(router, "_relay_retry", side_effect=mint) as fetch:
            first, _ = router._fetch_token("a", None, "a")
            self.assertEqual(router._fetch_token("a", None, "a")[0], first)
            router._fetch_token("b", None, "b")
            router._fetch_token("a", "client", "a|client_id=client")
            self.assertEqual(fetch.call_count, 3)
            now[0] = 1700
            refreshed, _ = router._fetch_token("a", None, "a")
            self.assertEqual(fetch.call_count, 4)
            self.assertEqual(refreshed["expires_on"], 2700)
