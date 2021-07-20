from meli_challenge.westeros_graph import WesterosGraph
from meli_challenge.sources import CSVSource
from meli_challenge.sessions import CustomSparkSession
from typing import List
import argparse


def print_aggregate_interactions(graph: WesterosGraph) -> None:
    result = graph.get_aggregated_interactions()
    for row in result.collect():
        print("{character}\t{book_1},{book_2},{book_3},{total}".format(**row.asDict()))


def print_mutual_friends(graph: WesterosGraph, mutual_friends: List[str]) -> None:
    result = graph.get_mutual_friends(*mutual_friends)
    for row in result.collect():
        print(
            "{character_1}\t{character_2}\t{mutual}".format(
                **row.asDict(), mutual=",".join(row["mutual_friends"])
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_file_path", help="Input dataset file path. Must be a .csv file."
    )
    parser.add_argument(
        "--aggregate_interactions",
        action="store_true",
        help="Prints each character aggregated interactions.",
    )
    parser.add_argument(
        "--mutual_friends",
        nargs=2,
        metavar=("character 1", "character 2"),
        help="Prints the mutual friends of the two desired characters.",
    )
    args = parser.parse_args()

    custom_session = CustomSparkSession(
        app_name="meli_challenge_cli",
        configs=[("spark.jars", "assets/jars/graphframes-0.8.1-spark3.0-s_2.12.jar")],
        py_files=["assets/jars/graphframes-0.8.1-spark3.0-s_2.12.jar"],
        log_level="WARN",
    )
    csv_source = CSVSource(
        custom_session.spark_session,
        args.input_file_path,
        read_options={"header": "false", "delimiter": ",", "inferSchema": "false"},
    )
    graph = WesterosGraph(custom_session, csv_source)

    if args.aggregate_interactions:
        print_aggregate_interactions(graph)
    else:
        print_mutual_friends(graph, args.mutual_friends)
