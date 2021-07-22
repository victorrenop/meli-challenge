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
    """Adds interactions to the graph based on a post request.

    The body of the request must contain the input interaction and the data must follow
    the following rules: have same schema as the defined standard (located at the
    payload_schema.json file) and only have books that are allowed to be inserted, those
    being defined by the challenge as being only the book number 4. If those rules are
    not followed, the request fails and returns a status code 400.

    Returns:
        Response: Flask response object cotaining a success message and status 201 if
            the request is a success, else returns a response object containing the
            failure message and status 400.

    """

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
    """Gets the list of common friends between two characters.

    The characters are passed to the request as URL parameters following the pattern
    "?source=SOURCE_CHARACTER_NAME&target=TARGET_CHARACTER_NAME".

    The request must have both source and targe character or else the status code 400
    is returned. If no common friends are found, an empty list is returned.

    Returns:
        Response: Response object with an error message and status 400 if no source or
            target character are provided, else returns a json payload with the common
            friends list.
    """

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
