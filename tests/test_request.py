import pytest
import requests
from unittest.mock import Mock, patch
from tenacity import wait_none

from ingestion.ingestion_functions import verified_request, HTTPErrorToRetry


def make_response(status, json_data=None):
    r = Mock()
    r.status_code = status
    r.ok = status < 400
    r.text = ""
    r.json.return_value = json_data
    if status >= 400:
        r.raise_for_status.side_effect = requests.exceptions.HTTPError(str(status))
    return r


@pytest.fixture(autouse=True)
def no_wait(monkeypatch):
    # cancels the sleeping times
    monkeypatch.setattr(verified_request.retry, "wait", wait_none())


@patch("requests.get")
def test_429_then_success(mock_get):
    mock_get.side_effect = [make_response(429), make_response(200, {"ok": True})]
    assert verified_request("http://x") == {"ok": True}
    assert mock_get.call_count == 2


@patch("requests.get")
def test_429_permanent(mock_get):
    mock_get.return_value = make_response(429)
    with pytest.raises(HTTPErrorToRetry):
        verified_request("http://x")
    assert mock_get.call_count == 3


@patch("requests.get")
def test_404_no_retry(mock_get):
    mock_get.return_value = make_response(404)
    with pytest.raises(requests.exceptions.HTTPError):
        verified_request("http://x")
    assert mock_get.call_count == 1


@patch("requests.get")
def test_timeout_is_retried(mock_get):
    mock_get.side_effect = requests.exceptions.Timeout
    with pytest.raises(requests.exceptions.Timeout):
        verified_request("http://x")
    assert mock_get.call_count == 3


@patch("requests.get")
def test_503_permanent(mock_get):
    mock_get.return_value = make_response(503)
    with pytest.raises(HTTPErrorToRetry):
        verified_request("http://x")
    assert mock_get.call_count == 3

