from meli_challenge.westeros_graph import WesterosGraph
from meli_challenge.sources import ObjectSource
from pyspark.sql import Row


class TestWesterosGraph:
    def test_aggregate_interactions(self, custom_session):
        input_test_data = [
            ["Addam-Marbrand", "Jaime-Lannister", 3, 1],
            ["Addam-Marbrand", "Tywin-Lannister", 6, 1],
            ["Aegon-I-Targaryen", "Daenerys-Targaryen", 5, 1],
            ["Aegon-I-Targaryen", "Eddard-Stark", 2, 1],
        ]
        expected_result = [
            Row(character="Addam-Marbrand", book_1=9, book_2=0, book_3=0, total=9),
            Row(character="Aegon-I-Targaryen", book_1=7, book_2=0, book_3=0, total=7),
            Row(character="Tywin-Lannister", book_1=6, book_2=0, book_3=0, total=6),
            Row(character="Daenerys-Targaryen", book_1=5, book_2=0, book_3=0, total=5),
            Row(character="Jaime-Lannister", book_1=3, book_2=0, book_3=0, total=3),
            Row(character="Eddard-Stark", book_1=2, book_2=0, book_3=0, total=2),
        ]
        object_reader = ObjectSource(
            custom_session.spark_session,
            input_test_data,
        )
        graph = WesterosGraph(custom_session, object_reader)

        actual_result = graph.get_aggregated_interactions().collect()

        assert expected_result == actual_result

    def test_mutual_friends(self, custom_session):
        input_test_data = [
            ["Addam-Marbrand", "Jaime-Lannister", 3, 1],
            ["Addam-Marbrand", "Tywin-Lannister", 6, 1],
            ["Eddard-Stark", "Jaime-Lannister", 3, 1],
            ["Eddard-Stark", "Tywin-Lannister", 6, 1],
            ["Aegon-I-Targaryen", "Tywin-Lannister", 6, 1],
            ["Eddard-Stark", "Tywin-Lannister", 3, 1],
            ["Eddard-Stark", "Aegon-I-Targaryen", 3, 1],
        ]
        expected_result = [
            Row(
                character_1="Jaime-Lannister",
                character_2="Tywin-Lannister",
                mutual_friends=["Addam-Marbrand", "Eddard-Stark"],
            ),
        ]
        object_reader = ObjectSource(
            custom_session.spark_session,
            input_test_data,
        )
        graph = WesterosGraph(custom_session, object_reader)

        actual_result = graph.get_mutual_friends(
            "Jaime-Lannister", "Tywin-Lannister"
        ).collect()
        for row in actual_result:
            row["mutual_friends"].sort()

        assert expected_result == actual_result

    def test_all_characters_mutual_friends(self, custom_session):
        input_test_data = [
            ["Addam-Marbrand", "Jaime-Lannister", 1, 1],
            ["Addam-Marbrand", "Tywin-Lannister", 1, 1],
            ["Jaime-Lannister", "Tywin-Lannister", 1, 1],
            ["Eddard-Stark", "Jaime-Lannister", 1, 1],
            ["Eddard-Stark", "Tywin-Lannister", 1, 1],
        ]
        expected_result = [
            Row(
                character_1="Addam-Marbrand",
                character_2="Eddard-Stark",
                mutual_friends=["Jaime-Lannister", "Tywin-Lannister"],
            ),
            Row(
                character_1="Addam-Marbrand",
                character_2="Jaime-Lannister",
                mutual_friends=["Tywin-Lannister"],
            ),
            Row(
                character_1="Addam-Marbrand",
                character_2="Tywin-Lannister",
                mutual_friends=["Jaime-Lannister"],
            ),
            Row(
                character_1="Eddard-Stark",
                character_2="Jaime-Lannister",
                mutual_friends=["Tywin-Lannister"],
            ),
            Row(
                character_1="Eddard-Stark",
                character_2="Tywin-Lannister",
                mutual_friends=["Jaime-Lannister"],
            ),
            Row(
                character_1="Jaime-Lannister",
                character_2="Tywin-Lannister",
                mutual_friends=["Addam-Marbrand", "Eddard-Stark"],
            ),
        ]
        object_reader = ObjectSource(
            custom_session.spark_session,
            input_test_data,
        )
        graph = WesterosGraph(custom_session, object_reader)

        actual_result = graph.get_all_characters_mutual_friends().collect()
        actual_result.sort(key=lambda x: (x["character_1"], x["character_2"]))
        for row in actual_result:
            row["mutual_friends"].sort()

        assert expected_result == actual_result

    def test_empty_source_graph_creation(self, custom_session):
        graph = WesterosGraph(custom_session)

        assert graph._graph.vertices.count() == 0
        assert graph._graph.edges.count() == 0

    def test_empty_source_agg_interactions(self, custom_session):
        graph = WesterosGraph(custom_session)

        assert graph.get_aggregated_interactions().count() == 0

    def test_empty_source_mutual_friends(self, custom_session):
        graph = WesterosGraph(custom_session)

        assert graph.get_mutual_friends("mock_1", "mock_2").count() == 0
