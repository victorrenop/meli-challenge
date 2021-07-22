from meli_challenge.sessions import CustomSparkSession
from meli_challenge.westeros_graph import WesterosGraph
from meli_challenge.sources import ObjectSource
from meli_challenge.utils.common_functions import (
    check_json_object_schema,
    schema_file_to_schema_object,
)
from meli_challenge.utils.constants import (
    APP_HOST,
    APP_PORT,
    PAYLOAD_MAPPING,
    PAYLOAD_SCHEMA,
)
from flask import Flask, request, Response, jsonify


SESSION = CustomSparkSession(
    app_name="meli_challenge_api",
    configs=[("spark.jars", "assets/jars/graphframes-0.8.1-spark3.0-s_2.12.jar")],
    py_files=["assets/jars/graphframes-0.8.1-spark3.0-s_2.12.jar"],
    log_level="WARN",
)
GRAPH = WesterosGraph(SESSION)
BOOKS = [4]

app = Flask("westeros_api")


@app.route("/interactions", methods=["POST"])
def add_interactions_to_graph():
    if not check_json_object_schema(request.json, PAYLOAD_MAPPING):
        return Response(
            "Invalid payload schema!\n"
            + "Expected schema: %s" % (schema_file_to_schema_object(PAYLOAD_SCHEMA)),
            status=400,
            mimetype="application/json",
        )
    if request.json["book"] not in BOOKS:
        return Response(
            "Invalid book number in payload!\n" + "Expected books: %s" % (BOOKS),
            status=400,
            mimetype="application/json",
        )

    object_source = ObjectSource(SESSION.spark_session, [request.json])
    input_df = object_source.read(PAYLOAD_SCHEMA)
    GRAPH.build_graph(input_df, ("source", "target"))

    return Response(
        "Successfully added interaction", status=201, mimetype="application/json"
    )


@app.route("/common-friends", methods=["GET"])
def get_common_friends():
    source = request.args.get("source")
    target = request.args.get("target")
    if not source or not target:
        return Response(
            "Please provide both source and target characters!",
            status=400,
            mimetype="application/json",
        )
    common_friends = GRAPH.get_mutual_friends(source, target).collect()
    common_friends = (
        common_friends[0]["mutual_friends"] if len(common_friends) > 0 else []
    )

    return jsonify({"common_friends": common_friends}), 200


if __name__ == "__main__":
    app.run(APP_HOST, APP_PORT)
