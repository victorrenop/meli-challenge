# Mercado Livre Challenge

## Challenge Definition

The objective of this challenge is to analyze interaction networks between the characters from the books of the saga A Song of Ice and Fire, written by George R. R. Martin.

There are 3 challenges to complete:
* Compute the number of interactions in each of the first series books and the sum of all interactions
* Compute the number of common friends between all character pairs
* Build a REST API that:
    * Has a POST endpoint "/interaction" that inserts an interaction between two characters
    * Has a GET endpoint "/common-friends" that accepts two parameters with the format `GET -> /common-friends?source=NOME_DO_P1&target=NOME_DO_P2` and returns a list of common friends between the characters.

## The Dataset

The dataset is the formated list of interactions from the first three books in the saga, based on the list created by Andrew Beveridge (https://github.com/mathbeveridge/asoiaf) and is located at `assets/input_data/dataset.csv`.

## Technology Choice

To solve this problem, the technology used is:
* `Spark`: Highly parallelizable and scalable, perfect for big and small applications. Since it can be run on any environment, anything that runs locally will run at a cluster. Spark has been proved to be, for many years, one of the best choices for big data and large data processing and, because of this, Spark was chosen as the backbone of this challenge solution. Since its engine can run on a scalable cluster, any code written using the Spark backend will scale with the cluster.
* `PySpark`: Basic Python API for dealing with the Spark backend. Has a very intuitive API and leaves all of the hard work to the Spark backend.
* `GraphFrames`: Package for Spark that provides DataFrame based graphs and has an API for Python, integrating with the rest of the solution. It unites the DataFrame interface with the functionality of GraphX. Since all of the graph processing is done by Spark, this scales with Spark's deploy environment, providing a nice balance between easy to use APIs, heavy load graph processing and scalability.
* `Flask`: Basic choice for REST API building in Python, perfect for the third challenge.
* `Docker`: Used to build the container for the REST API.

## The Solution

To solve the first two challenges, the input dataset must be transformed in a Graph Frames graph. To do this, vertices and edges DataFrames must be created. Graph Frames demands that the vertices DataFrame must obligatory contain the column "id" that defines the name of each vertex on the graph and that the edges DataFrame must contain the columns "src" and "dst" that defines from where the edge is coming from and where it's going to.

Since each row contains a relationship definition, to build the vertices DataFrame, an union is made between two auxiliary DataFrames: one with the source column and the other with the target column, resulting in a DataFrame with all characters.

To build the edges DataFrame, first the interactions with weight 0 are filtered out because they don't characterize an actual interaction. Then an union is made with two auxiliary DataFrames: the first is simply the original DataFrame and the second contains the same interactions of the original DataFrame, but inverted. This is needed because an interaction is bi-directional.

With these, the graph is ready to be used. To solve the first challenge, a group by and pivot is needed to compute and output what is asked for. The pivot is needed because the original summarization doesn't return the result in the desired format, so the rows transposition is needed.

To solve the second challenge, a motif query is made in the Graph. This motif query simply states "(a)-[]->[b]; (a)-[]->[c]", meaning that we want a vertex "a" that goes to "b" and "c", a mutual friend. But Graph Frame works in a way that the results have duplicated records and inverted duplicate records, that need to be later filtered out. Since the edges were created bi-directional, this motif is enough to find all mutual friends.

The last challenge relies on Flask with two simple functions to parse the desired endpoints. The POST endpoint validates the payload schema and appends the new results to the graph's vertices and edges DataFrames, recreating the graph in the process. The GET endpoint utilizes the same motif as the second challenge, but filters out all other results except of the desired character pair.

## How to Run

To run the project and test its functionality, go to the following [page](how_to_run.md)
