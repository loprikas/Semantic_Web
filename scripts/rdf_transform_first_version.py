import re
import pandas as pd
from os import listdir
from os.path import isfile, join
from rdflib import Dataset, URIRef, Literal, Namespace, RDF, RDFS, OWL, XSD
from iribaker import to_iri
from tqdm import tqdm

# Maximale Anzahl an Spalten, die angezeigt werden sollen
pd.set_option('display.max_columns', None)

# Breite der Anzeige
pd.set_option('display.width', 200)

# Spalteninhalte nicht abschneiden
pd.set_option('display.max_colwidth', None)

path = "../data"

# Nur .tsv.gz-Dateien finden und Endung abschneiden
files = [re.sub(r"\.tsv\.gz$", "", f) for f in listdir(path) if isfile(join(path, f)) and f.endswith(".tsv.gz")]

data_dict = {}

for file in files:
    file_path = f"{path}/{file}.tsv.gz"
    try:
        # Robustes Einlesen mit Tab-Trennung und Fehlerüberspringen
        df = pd.read_csv(
            file_path,
            sep="\t",
            compression="gzip",
            on_bad_lines="skip"  # Falls Pandas >= 1.3.0
        )
        data_dict[file] = df
        print(f"[✓] Erfolgreich geladen: {file}")
    except Exception as e:
        print(f"[!] Fehler beim Laden von '{file}': {e}")

# LOGIK


# Ausgabe der geladenen DataFrames
for key, value in data_dict.items():
    print(key)
    print(value.head())  # nur Kopf anzeigen, um Ausgabe übersichtlich zu halten

# ---------------------- TRANSFORMATION -------------------------------


# Prep #


# A namespace for our resources
data = 'http://imdb.aksw.org/ontology#'
DATA = Namespace(data)
# A namespace for our vocabulary items (schema information, RDFS, OWL classes and properties etc.)
vocab = 'http://imdb.aksw.org/ontology#'
VOCAB = Namespace(vocab)

# The URI for our graph
graph_uri = URIRef('http://imdb.aksw.org/ontology#')

# We initialize a dataset, and bind our namespaces
dataset = Dataset()
dataset.bind('imdb_data', DATA)
dataset.bind('imdb_vocab', VOCAB)

# We then get a new graph object with our URI from the dataset.
graph = dataset.graph(graph_uri)

# Load the externally defined schema into the default graph (context) of the dataset
dataset.default_context.parse('imdb_ontology.ttl', format='turtle')

# create URIs #
# classes #

# creating URIs for rating

for index, row in tqdm(data_dict["short.title.ratings"].iterrows()):
    rating = URIRef(to_iri(f"{data}Rating{row['tconst']}"))

    rating_number = Literal(row["averageRating"], datatype=XSD['string'])
    ratings_amount = Literal(row["numVotes"], datatype=XSD['string'])

    graph.add((rating, RDF.type, VOCAB['Rating']))

    graph.add((rating, VOCAB['averageRating'], rating_number))
    graph.add((rating, VOCAB['numVotes'], ratings_amount))

# creating URIs for alternate titles

for index, row in tqdm(data_dict["short.title.akas"].iterrows()):
    alternate_title = URIRef(to_iri(f"{data}Alternate Title{row['titleId']}-{row['region']}-{row['language']}"))

    a_title = Literal(row["title"], datatype=XSD['string'])
    region = Literal(row["region"], datatype=XSD['string'])
    language = Literal(row["language"], datatype=XSD['string'])

    graph.add((alternate_title, RDF.type, VOCAB['Alternate Title']))

    graph.add((alternate_title, VOCAB['alternateTitle'], a_title))
    graph.add((alternate_title, VOCAB['region'], region))
    graph.add((alternate_title, VOCAB['language'], language))


# creating URIs for persons

for index, row in tqdm(data_dict["short.name.basic"].iterrows()):
    person = URIRef(to_iri(f"{data}Person{row['nconst']}"))

    birth_year = Literal(row["birthYear"], datatype=XSD['string'])
    death_year = Literal(row["deathYear"], datatype=XSD['string'])
    name = Literal(row["primaryName"], datatype=XSD['string'])
    if "actor" in str(row["primaryProfession"]):
        gender = Literal("male", datatype=XSD['string'])
    elif "actress" in str(row["primaryProfession"]):
        gender = Literal("female", datatype=XSD['string'])
    else:
        gender = Literal("unknown", datatype=XSD['string'])

    graph.add((person, RDF.type, VOCAB['Person']))

    graph.add((person, VOCAB['year of birth'], birth_year))
    graph.add((person, VOCAB['year of death'], death_year))
    graph.add((person, VOCAB['gender'], gender))

for index, row in tqdm(data_dict["short.title.crew"].iterrows()):
  profession = URIRef(to_iri(f"{data}Profession{row['tconst']}"))

# creating URIs for episodes

for index, row in tqdm(data_dict["short.title.episode"].iterrows()):

# creating URIs for alternate titles

for index, row in tqdm(data_dict["short.title.principals"].iterrows()):
    tconst = row.get("tconst")
    nconst = row.get("nconst")


# creating URIs for alternate titles

for index, row in tqdm(data_dict["short.title.basics"].iterrows()):
    title = URIRef(to_iri(f"{data}Title{row['tconst']}"))

    runtime = Literal(row["runtimeMinutes"], datatype=XSD['string'])
    release_year = Literal(row["startYear"], datatype=XSD['string'])
    genres = Literal(row["genres"], datatype=XSD['string'])
    original_title = Literal(row["originalTitle"], datatype=XSD['string'])
    title_type = Literal(row["titleType"], datatype=XSD['string'])

    graph.add((title, RDF.type, VOCAB['Title']))

    graph.add((title, VOCAB['runtime'], runtime))
    graph.add((title, VOCAB['release year'], release_year))
    graph.add((title, VOCAB['genres'], genres))
    graph.add((title, VOCAB['title'], original_title))
    graph.add((title, VOCAB['type'], title_type))
# connections between classes #




# Test Print #

#print(graph.serialize(format='ttl'))

# save ttl file #

# save into turtle
# graph.serialize(format='ttl', destination='imdb_transformed.ttl')
