"""testing file"""


import unittest
import requests
import json

from dags.car_traffic_dag import parse_warning

class CarDataTest(unittest.TestCase):
    def test_parse_warning_01(self):
        input = {
            "identifier": "INRIX--vi-zus.2026-03-29_12-42-24-000.de0",
            "icon": "101",
            "isBlocked": "false",
            "future": False,
            "extent": "50.97550173680195,6.850192886597498,50.944820420071814,6.849452798098435",
            "point": "50.97550173680195,6.850192886597498",
            "startLcPosition": "74",
            "display_type": "WARNING",
            "subtitle": " Köln -> Euskirchen",
            "title": "A1 | Köln-Bocklemünd - Köln-Lövenich",
            "startTimestamp": "2026-03-29T12:42:24Z",
            "delayTimeValue": "22",
            "abnormalTrafficType": "UNSPECIFIED_ABNORMAL_TRAFFIC",
            "coordinate": {
                "lat": 50.97550173680195,
                "long": 6.850192886597498
            },
            "description": [
                "Beginn: 29.03.26 um 14:42 Uhr",
                "",
                "Zusammengesetzte Verkehrsinformation, seit 29.03.2026, 12:42",
                "A1: Köln -> Euskirchen, zwischen 0.3 km hinter AS Köln-Bocklemünd und 0.8 km vor AS Köln-Lövenich",
                "",
                "Ereignismeldung:",
                "- Reisezeitverlust: 22 Minuten"
            ],
            "routeRecommendation": [],
            "footer": [],
            "lorryParkingFeatureIcons": [],
            "source": "inrix",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                [
                    6.850192887,
                    50.975501737
                ],
                [
                    6.8501851,
                    50.975467301
                ]
                ]
            }
        }
        output = {
            "id": "INRIX--vi-zus.2026-03-29_12-42-24-000.de0",
            "title": "A1 | Köln-Bocklemünd - Köln-Lövenich",
            "description": "Beginn: 29.03.26 um 14:42 Uhr\n\nZusammengesetzte Verkehrsinformation, seit 29.03.2026, 12:42\nA1: Köln -> Euskirchen, zwischen 0.3 km hinter AS Köln-Bocklemünd und 0.8 km vor AS Köln-Lövenich\n\nEreignismeldung:\n- Reisezeitverlust: 22 Minuten",
            "latitude": 50.97550173680195,
            "longitude": 6.850192886597498
        }
        self.assertDictEqual(parse_warning(input), output)
    
    def test_parse_warning_02(self):
        input = {
            "identifier": "INRIX--vi-zus.2026-03-29_12-42-24-000.de0",
            "title": "A1 | Köln-Bocklemünd - Köln-Lövenich",
            "coordinate": {
                "lat": 50.97550173680195,
                "long": 6.850192886597498
            },
            "description": []
        }
        output = {
            "id": "INRIX--vi-zus.2026-03-29_12-42-24-000.de0",
            "title": "A1 | Köln-Bocklemünd - Köln-Lövenich",
            "description": "",
            "latitude": 50.97550173680195,
            "longitude": 6.850192886597498
        }
        self.assertDictEqual(parse_warning(input), output)

class CarAPITest(unittest.TestCase):
    def test_api_response(self):
        resp = requests.get("https://verkehr.autobahn.de/o/autobahn/")
        self.assertTrue(resp.ok) # Check for api connection

        data = json.loads(resp.content)
        self.assertTrue("roads" in data) # Check for response schema
        
        self.assertTrue(all(road in data["roads"] for road in ["A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"])) # Check for major roads