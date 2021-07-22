from meli_challenge.westeros_api import app, GRAPH, SESSION
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from meli_challenge.utils.constants import EDGES_SCHEMA, VERTICES_SCHEMA
from graphframes import GraphFrame
from pyspark.sql import Row
import pytest


@pytest.fixture(autouse=True)
def rebuild_graph_each_test_session():
    yield
    GRAPH._graph = GraphFrame(
        SESSION.spark_session.createDataFrame(
            SESSION.spark_context.emptyRDD(),
            schema_file_to_schema_object(VERTICES_SCHEMA),
        ),
        SESSION.spark_session.createDataFrame(
            SESSION.spark_context.emptyRDD(), schema_file_to_schema_object(EDGES_SCHEMA)
        ),
    )


@pytest.fixture
def client():
    with app.test_client() as client:
        yield client


class TestWesterosAPI:
    def test_add_interactions(self, client):
        test_payloads = [
            {
                "source": "Eddard-Stark",
                "target": "Tyrion-Lannister",
                "weight": 1,
                "book": 4,
            },
            {
                "source": "Eddard-Stark",
                "target": "Jaime-Lannister",
                "weight": 1,
                "book": 4,
            },
        ]
        expected_edges = [
            Row(src="Eddard-Stark", dst="Jaime-Lannister", weight=1, book=4),
            Row(src="Eddard-Stark", dst="Tyrion-Lannister", weight=1, book=4),
            Row(src="Jaime-Lannister", dst="Eddard-Stark", weight=1, book=4),
            Row(src="Tyrion-Lannister", dst="Eddard-Stark", weight=1, book=4),
        ]

        for payload in test_payloads:
            client.post("/interactions", json=payload)

        actual_edges = GRAPH._graph.edges.collect()
        actual_edges.sort(key=lambda x: (x["src"], x["dst"]))

        assert expected_edges == actual_edges

    def test_get_mutual_friends(self, client):
        test_payloads = [
            {
                "source": "Eddard-Stark",
                "target": "Tyrion-Lannister",
                "weight": 1,
                "book": 4,
            },
            {
                "source": "Eddard-Stark",
                "target": "Jaime-Lannister",
                "weight": 1,
                "book": 4,
            },
        ]
        expected_result = {"common_friends": ["Eddard-Stark"]}

        for payload in test_payloads:
            client.post("/interactions", json=payload)
        actual_result = client.get(
            "/common-friends?source=Tyrion-Lannister&target=Jaime-Lannister"
        ).json

        assert expected_result == actual_result

    def test_no_mutual_friends(self, client):
        expected_result = {"common_friends": []}

        actual_result = client.get(
            "/common-friends?source=Tyrion-Lannister&target=Jaime-Lannister"
        ).json

        assert expected_result == actual_result

    @pytest.mark.parametrize(
        "parameters",
        [
            ("?source=Tyrion-Lannister"),
            ("?target=Tyrion-Lannister"),
            (""),
        ],
    )
    def test_mutual_friends_missing_parameters(self, client, parameters):
        expected_status_code = 400

        actual_status_code = client.get("/common-friends%s" % (parameters)).status_code

        assert expected_status_code == actual_status_code

    @pytest.mark.parametrize(
        "payload",
        [
            ({"target": "Tyrion-Lannister", "weight": 1, "book": 4}),
            (
                {
                    "source": "Eddard-Stark",
                    "target": "Tyrion-Lannister",
                    "weight": "1",
                    "book": 4,
                }
            ),
        ],
    )
    def test_add_interaction_invalid_schema(self, client, payload):
        payload = {"target": "Tyrion-Lannister", "weight": 1, "book": 4}
        expected_status_code = 400

        actual_status_code = client.post("/interactions", json=payload).status_code

        assert expected_status_code == actual_status_code

    def test_add_interaction_invalid_book(self, client):
        payload = {
            "source": "Eddard-Stark",
            "target": "Tyrion-Lannister",
            "weight": 1,
            "book": 1,
        }
        expected_status_code = 400

        actual_status_code = client.post("/interactions", json=payload).status_code

        assert expected_status_code == actual_status_code
