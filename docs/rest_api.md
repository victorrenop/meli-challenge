# Westeros REST API

## `GET /common-friends`
Returns the common friends list between two characters.

Returns 201 with the common friends list as a json.

Returns 400 if no source or target is provided.

### Parameters
- `source`: First character's name, must be a string. Case insensitive.
- `target`: Second character's name, must be a string. Case insensitive.

### Examples
Valid request:
```bash
$ curl -X GET "http://localhost:5000/common-friends?source=Jon-Snow&target=Tywin-Lannister"
```

Response:
```
{
    "common_friends": [
        "Cersei-Lannister","Arya-Stark"
    ]
}
```

## `POST /interactions`
Adds the desired interaction in the graph.

Returns 201 if the interaction was successfully created.

Returns 400 if there was a schema error with the payload or if a invalid book was provided.

### Parameters
- `source`: First character's name, must be a string.
- `target`: Second character's name, must be a string.
- `weight`: Number of interactions between the source and target characters, must be an integer.
- `book`: Book number, must be and integer equal to 4.

### Examples
Valid request:
```bash
curl -i -X POST -H "content-type: application/json" -d '{"source": "Jon-Snow", "target": "Tywin-Lannister", "weight": 1, "book": 4}' http://localhost:5000/interactions
```

Response:
```
HTTP/1.0 201 CREATED
Content-Type: application/json
Content-Length: 30
Server: Werkzeug/2.0.1 Python/3.8.5

Successfully added interaction
```
