import pytest
import json
import copy
from ingestion.periodic_ingestion import build_flux_rows

TEST_STOPS = {'stop_point:SNCF:87571240:Train', 'stop_point:SNCF:87571240:Coach', 'stop_point:SNCF:87571240:RailShuttle'}
TEST_AREAS = {'stop_area:SNCF:87671339', 'stop_area:SNCF:87574897', 'stop_area:SNCF:87571240', 'stop_area:SNCF:87391003', 'stop_area:SNCF:87571000'}
TEST_LINES = {'line:SNCF:FR:Line::85d3579c-996b-4671-89af-839855ede78a:', 'line:SNCF:FR:Line::0B0B302C-2E5D-4AB4-BE84-4DA893F7C926:', 'line:stop_area:SNCF:87671339_stop_area:SNCF:87391003'}
TEST_NETWORKS = {'network:SNCF:FR:Branding::d5d601cf-e383-4138-8e70-ee1b7fcadd48:', 'network:additional_service', 'network:SNCF:SNF'}
known_ids = {"stations": TEST_STOPS, "areas": TEST_AREAS, "lines": TEST_LINES, "networks": TEST_NETWORKS}

def load_dicts(file):
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)

def test_deduplication():
    arr_data = load_dicts("tests/data/arr_data_test.json")
    dep_data = load_dicts("tests/data/dep_data_test.json")
    dedup_data, stats = build_flux_rows(dep_data, arr_data, known_ids=None)
    assert len(dedup_data) == 5

def test_ignore_missing(monkeypatch):
    arr_data = load_dicts("tests/data/arr_data_test.json")
    dep_data = load_dicts("tests/data/dep_data_test.json")
    monkeypatch.setitem(arr_data["arrivals"][0], "links", [])
    monkeypatch.setitem(dep_data["departures"][0], "links", [])
    dedup_data, stats = build_flux_rows(dep_data, arr_data, known_ids=None)
    assert stats["incomplete"] == 2

@pytest.mark.parametrize("kind, removed", [
    ("stations", "stop_point:SNCF:87571240:Train"),
    ("areas", "stop_area:SNCF:87671339"),
    ("lines", "line:SNCF:FR:Line::85d3579c-996b-4671-89af-839855ede78a:"),
    ("networks", "network:SNCF:FR:Branding::d5d601cf-e383-4138-8e70-ee1b7fcadd48:"),
])
def test_ignore_unknown_reference(kind, removed):
    arr_data = load_dicts("tests/data/arr_data_test.json")
    dep_data = load_dicts("tests/data/dep_data_test.json")
    known = copy.deepcopy(known_ids)
    known[kind].discard(removed)          # on « oublie » un seul identifiant
    _, stats = build_flux_rows(dep_data, arr_data, known_ids=known)
    assert stats["unknown_ref"] == 2
    assert stats["unknown_ids"] == {removed}