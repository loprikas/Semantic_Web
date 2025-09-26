# ---------------------- TRANSFORMATION (RAM-schonend, Reinit pro Flush) -----------------------
# [ÄNDERUNG] Dateiliste: wir verwenden die gefundenen Dateinamen 1:1, ohne Suffix-Manipulation, um ".tsv.gz.tsv.gz" zu vermeiden.
# [ÄNDERUNG] sep="\t" statt "\\t" (verhindert ParserWarning).
# [ÄNDERUNG] _flush_graph() führt IMMER einen harten Reset durch (kein langsames graph.remove mehr).
# [ÄNDERUNG] Preview stark reduziert und sofort freigegeben.

from rdflib import Dataset, URIRef, Namespace, Literal, XSD, RDFS, RDF
from iribaker import to_iri
import pandas as pd
import re
from os import listdir
from os.path import isfile, join
from tqdm import tqdm
import gc

# Namespaces
RES = Namespace('http://example.org/imdb/resource/')
IMD = Namespace('http://example.org/imdb#')

# Hilfsfunktion: neuen Graph/Dataset erzeugen und Namespaces binden
def _new_graph():
    global dataset, graph
    dataset = Dataset()
    dataset.bind('res', RES)
    dataset.bind('imd', IMD)
    graph = dataset.default_context

# Graph initialisieren
_new_graph()

# Streaming-Ziel und Flush-Schwelle
OUT_TTL = 'imdb_transformed.ttl'
FLUSH_TRIPLES = 200_000

# [ÄNDERUNG] Tripel anhängen und IMMER hart resetten, um Speicherfragmentierung zu vermeiden
def _flush_graph():
    global dataset, graph
    if len(graph):
        data = graph.serialize(format='nt')
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        with open(OUT_TTL, 'a', encoding='utf-8') as f:
            f.write(data)
    # harter Reset statt graph.remove((None,None,None))
    try:
        dataset.close()
    except Exception:
        pass
    del graph
    del dataset
    gc.collect()
    _new_graph()

# Turtle-Prefixe einmalig schreiben
with open(OUT_TTL, 'w', encoding='utf-8') as f:
    f.write('@prefix res: <http://example.org/imdb/resource/> .\n')
    f.write('@prefix imd: <http://example.org/imdb#> .\n\n')

# Ontologie laden und sofort persistieren
dataset.default_context.parse('imdb_ontology.ttl', format='turtle')
_flush_graph()  # [ÄNDERUNG]

# Pfad zu den IMDb-Daten
path = "../uncutted files"

# [ÄNDERUNG] Alle .tsv.gz-Dateien unverändert aufnehmen
files = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith(".tsv.gz")]

# [ÄNDERUNG] Mini-Preview: nur Dateinamen ausgeben, kein DataFrame lesen
print("[Info] Gefundene Dateien:")
for f in files:
    print(" -", f)

# [ÄNDERUNG] Chunk-Iterator akzeptiert den echten Dateinamen
def _iter_chunks(filename):
    file_path = f"{path}/{filename}"
    yield from pd.read_csv(
        file_path,
        sep="\t",                 # [ÄNDERUNG] keine Regex, keine Warnung
        compression="gzip",
        on_bad_lines="skip",
        chunksize=100_000
    )

# [ÄNDERUNG] Dateiname -> logische Basis (entscheidet Routing)
def _base(name: str) -> str:
    # Entfernt nur die Endung .tsv.gz, ohne den Rest zu verändern
    return re.sub(r"\.tsv\.gz$", "", name)

# -------- Title (basics) --------
for df in _iter_chunks("title.basics.tsv.gz"):
    for _, row in tqdm(df.iterrows(), total=len(df)):
        tconst = row["tconst"]
        t = URIRef(to_iri(f"{RES}title/{tconst}"))
        graph.add((t, RDF.type, IMD.Title))

        tt = "" if pd.isna(row.get("titleType")) else str(row.get("titleType"))
        if tt == "tvSeries":
            graph.add((t, RDF.type, IMD.TVSeries))
        if tt == "tvEpisode":
            graph.add((t, RDF.type, IMD.Episode))

        graph.add((t, IMD.titleID, Literal(str(tconst), datatype=XSD.string)))

        v = row.get("primaryTitle")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            graph.add((t, IMD.primaryTitle, Literal(str(v), datatype=XSD.string)))

        v = row.get("originalTitle")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            graph.add((t, IMD.originalTitle, Literal(str(v), datatype=XSD.string)))

        v = row.get("isAdult")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            s = str(v).lower()
            b = True if s in {'1', 'true', 't'} else False
            graph.add((t, IMD.isAdult, Literal(b, datatype=XSD.boolean)))

        v = row.get("startYear")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((t, IMD.startYear, Literal(str(v), datatype=XSD.string)))
            except:
                pass

        v = row.get("endYear")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((t, IMD.endYear, Literal(str(v), datatype=XSD.string)))
            except:
                pass

        v = row.get("runtimeMinutes")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((t, IMD.runtimeMinutes, Literal(int(v), datatype=XSD.integer)))
            except:
                pass

        v = row.get("titleType")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            graph.add((t, IMD.type, Literal(str(v), datatype=XSD.string)))

        v = row.get("genres")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            for g_ in str(v).split(','):
                g = g_.strip()
                if g:
                    graph.add((t, IMD.genre, Literal(g, datatype=XSD.string)))
        # [ÄNDERUNG] Nur am Chunkende flushen, nicht zwischendurch
    _flush_graph()
    del df
    gc.collect()

# -------- Ratings --------
for df in _iter_chunks("title.ratings.tsv.gz"):
    for _, row in tqdm(df.iterrows(), total=len(df)):
        tconst = row["tconst"]
        r = URIRef(to_iri(f"{RES}rating/{tconst}"))
        graph.add((r, RDF.type, IMD.Rating))

        v = row.get("averageRating")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((r, IMD.averageRating, Literal(str(float(v)), datatype=XSD.decimal)))
            except:
                pass

        v = row.get("numVotes")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((r, IMD.numVotes, Literal(int(v), datatype=XSD.integer)))
            except:
                pass

        t = URIRef(to_iri(f"{RES}title/{tconst}"))
        graph.add((t, IMD.hasRating, r))
    _flush_graph()
    del df
    gc.collect()

# -------- Alternate titles (akas) --------
for df in _iter_chunks("title.akas.tsv.gz"):
    for _, row in tqdm(df.iterrows(), total=len(df)):
        titleId = row["titleId"]
        ordering = row.get("ordering")
        a = URIRef(to_iri(f"{RES}akas/{titleId}/{ordering}"))
        graph.add((a, RDF.type, IMD.AlternateTitle))

        if not pd.isna(ordering):
            try:
                graph.add((a, IMD.order, Literal(int(ordering), datatype=XSD.integer)))
            except:
                pass

        v = row.get("title")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            graph.add((a, IMD.alternateTitle, Literal(str(v), datatype=XSD.string)))

        v = row.get("region")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            graph.add((a, IMD.region, Literal(str(v), datatype=XSD.string)))

        v = row.get("language")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            graph.add((a, IMD.language, Literal(str(v), datatype=XSD.string)))

        t = URIRef(to_iri(f"{RES}title/{titleId}"))
        graph.add((t, IMD.hasAlternateTitle, a))
    _flush_graph()
    del df
    gc.collect()

# -------- Episodes --------
for df in _iter_chunks("title.episode.tsv.gz"):
    for _, row in tqdm(df.iterrows(), total=len(df)):
        tconst = row["tconst"]
        t = URIRef(to_iri(f"{RES}title/{tconst}"))
        graph.add((t, RDF.type, IMD.Episode))

        v = row.get("seasonNumber")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((t, IMD.seasonNumber, Literal(int(v), datatype=XSD.integer)))
            except:
                pass

        v = row.get("episodeNumber")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((t, IMD.episodeNumber, Literal(int(v), datatype=XSD.integer)))
            except:
                pass

        parent = row.get("parentTconst")
        if not pd.isna(parent) and str(parent) not in {r'\N', '\\N', ''}:
            p = URIRef(to_iri(f"{RES}title/{parent}"))
            graph.add((t, IMD.parentSeries, p))
    _flush_graph()
    del df
    gc.collect()

# -------- Persons --------
for df in _iter_chunks("name.basics.tsv.gz"):
    for _, row in tqdm(df.iterrows(), total=len(df)):
        nconst = row["nconst"]
        p = URIRef(to_iri(f"{RES}person/{nconst}"))
        graph.add((p, RDF.type, IMD.Person))
        graph.add((p, IMD.personID, Literal(str(nconst), datatype=XSD.string)))

        v = row.get("primaryName")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            graph.add((p, RDFS.label, Literal(str(v), datatype=XSD.string)))

        v = row.get("birthYear")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((p, IMD.birthYear, Literal(str(v), datatype=XSD.string)))
            except:
                pass

        v = row.get("deathYear")
        if not pd.isna(v) and str(v) not in {r'\N', '\\N', ''}:
            try:
                graph.add((p, IMD.deathYear, Literal(str(v), datatype=XSD.string)))
            except:
                pass

        prof = "" if pd.isna(row.get("primaryProfession")) else str(row.get("primaryProfession"))
        if "actress" in prof:
            graph.add((p, IMD.gender, Literal("female", datatype=XSD.string)))
        elif "actor" in prof:
            graph.add((p, IMD.gender, Literal("male", datatype=XSD.string)))

        kf = row.get("knownForTitles")
        if not pd.isna(kf) and str(kf) not in {r'\N', '\\N', ''}:
            for tt in str(kf).split(","):
                tt_ = tt.strip()
                if tt_:
                    t = URIRef(to_iri(f"{RES}title/{tt_}"))
                    graph.add((p, IMD.knownFor, t))
    _flush_graph()
    del df
    gc.collect()

# -------- Roles aus crew --------
for df in _iter_chunks("title.crew.tsv.gz"):
    for _, row in tqdm(df.iterrows(), total=len(df)):
        tconst = row["tconst"]
        for role_name, col in [("director", "directors"), ("writer", "writers")]:
            vals = row.get(col)
            if pd.isna(vals) or str(vals) in {r'\N', '\\N', ''}:
                continue
            for n in str(vals).split(","):
                nconst = n.strip()
                if not nconst:
                    continue
                person = URIRef(to_iri(f"{RES}person/{nconst}"))
                role = URIRef(to_iri(f"{RES}role/{tconst}/{role_name}/{nconst}"))
                t = URIRef(to_iri(f"{RES}title/{tconst}"))

                graph.add((role, RDF.type, IMD.Role))
                graph.add((role, IMD.roleName, Literal(role_name, datatype=XSD.string)))
                graph.add((person, IMD.hasRole, role))
                graph.add((role, IMD.roleIn, t))
    _flush_graph()
    del df
    gc.collect()

# -------- Roles aus principals --------
for df in _iter_chunks("title.principals.tsv.gz"):
    for _, row in tqdm(df.iterrows(), total=len(df)):
        tconst = row.get("tconst")
        nconst = row.get("nconst")
        if pd.isna(tconst) or str(tconst) in {r'\N', '\\N', ''} or pd.isna(nconst) or str(nconst) in {r'\N', '\\N', ''}:
            continue

        cat = "" if pd.isna(row.get("category")) else str(row.get("category")).strip()
        if cat == "actress":
            cat = "actor"
        if not cat:
            job = row.get("job")
            cat = "" if pd.isna(job) else str(job).strip()
        if not cat:
            cat = "role"

        person = URIRef(to_iri(f"{RES}person/{nconst}"))
        role = URIRef(to_iri(f"{RES}role/{tconst}/{cat}/{nconst}"))
        t = URIRef(to_iri(f"{RES}title/{tconst}"))

        graph.add((role, RDF.type, IMD.Role))
        graph.add((role, IMD.roleName, Literal(cat, datatype=XSD.string)))
        graph.add((person, IMD.hasRole, role))
        graph.add((role, IMD.roleIn, t))
    _flush_graph()
    del df
    gc.collect()

# Abschluss
_flush_graph()
print(f"[✓] Fertig. Tripel in '{OUT_TTL}' geschrieben.")