import pytest


@pytest.fixture
def announcement_message():
    return {
        "type": "ris_message",
        "data": {
            "id": "37.49.232.7-01a1f9c20000000e",
            "timestamp": 1795191603.2,
            "host": "rrc21.ripe.net",
            "peer": "37.49.232.7",
            "peer_asn": "8218",
            "type": "UPDATE",
            "med": 0,
            "origin": "IGP",
            "path": [8218, 37100, 327708, 37133, 36909, 36909, 36909],
            "community": [[8218, 102], [8218, 20000], [8218, 20110]],
            "announcements": [
                {
                    "next_hop": "37.49.232.7",
                    "prefixes": ["41.220.128.0/20"],
                }
            ],
            "withdrawals": [],
        },
    }
