# -*- coding: utf-8 -*-
"""
Erzeugt ein repräsentatives IMDb-RDF-Sample mit max. 100k Tripeln.
Voraussetzung: Die originalen IMDb .tsv.gz liegen im Ordner IMDB_PATH.
Die Ontologie liegt als 'imdb_ontology.ttl' neben diesem Skript.

Enthaltene Klassen/Strukturen:
- imd:Title (+ Subtypen imd:TVSeries, imd:Episode)
- imd:Rating und Verknüpfung imd:hasRating
- imd:AlternateTitle + Verknüpfung imd:hasAlternateTitle
- Episoden-Relationen: imd:seasonNumber, imd:episodeNumber, imd:parentSeries
- imd:Person, Rollen-Knoten imd:Role mit imd:roleName, imd:roleIn, imd:hasRole
- Basis-Attribute aus basics, name.basics (Label, birthYear, deathYear, gender-Heuristik)
- knownFor-Verknüpfungen

Die Logik basiert auf deinen beiden ETL-Skripten, jedoch mit globalem Budget
und gezielter Auswahl pro Datei, damit alle Klassen vorkommen.
"""

from rdflib import Graph, URIRef, Namespace, Literal
from rdflib.namespace import RDF, RDFS, XSD
import pandas as pd
import gzip, os, re
from iribaker import to_iri
from typing import Set, Dict

# ------------------ Parameter ------------------
IMDB_PATH = "../uncutted files"   # Pfad zu den .tsv.gz
ONTOLOGY_FILE = "imdb_ontology.ttl"
OUT_FILE = "imdb_sample_100k.ttl"
MAX_TRIPLES = 100_000

# Sampling-Quoten (werden nur als Obergrenzen genutzt; Budget hat Vorrang)
SEED_TITLES_MAX         = 4000   # Startmenge Titel aus basics
AKAS_PER_TITLE_MAX      = 2
EPISODES_PER_SERIES_MAX = 10
PRINCIPALS_PER_TITLE    = 5
CREW_ROLES_PER_TITLE    = 4      # Regie+Autor*innen zusammen
PEOPLE_LIMIT            = 6000   # Obergrenze neu zu materialisierender Personen

# ------------------ Namespaces ------------------
RES = Namespace('http://example.org/imdb/resource/')
IMD = Namespace('http://example.org/imdb#')

# ------------------ Graph + Budget-Wrapper ------------------
g = Graph()
g.bind('res', RES)
g.bind('imd', IMD)

TRIPLE_BUDGET = MAX_TRIPLES

def add_t(s, p, o):
    """Triple hinzufügen und Budget prüfen."""
    global TRIPLE_BUDGET
    if TRIPLE_BUDGET <= 0:
        return False
    before = len(g)
    g.add((s, p, o))
    after = len(g)
    # nur reduzieren, wenn Tripel neu war
    if after > before:
        TRIPLE_BUDGET -= 1
    return TRIPLE_BUDGET > 0

def budget_ok():
    return TRIPLE_BUDGET > 0

# ------------------ Hilfen ------------------
def gz_read(path: str, usecols=None, chunksize=100_000):
    return pd.read_csv(path, sep="\t", compression="gzip",
                       on_bad_lines="skip", dtype=str,
                       usecols=usecols, chunksize=chunksize)

def norm_str(x):
    return None if x is None or pd.isna(x) or str(x) in {r"\N", "\\N", ""} else str(x)

def iri_title(tconst: str) -> URIRef:
    return URIRef(to_iri(f"{RES}title/{tconst}"))

def iri_person(nconst: str) -> URIRef:
    return URIRef(to_iri(f"{RES}person/{nconst}"))

def iri_role(tconst: str, role_name: str, nconst: str) -> URIRef:
    return URIRef(to_iri(f"{RES}role/{tconst}/{role_name}/{nconst}"))

def iri_aka(titleId: str, ordering: str) -> URIRef:
    return URIRef(to_iri(f"{RES}akas/{titleId}/{ordering}"))

# ------------------ 0) Ontologie laden ------------------
if os.path.exists(ONTOLOGY_FILE):
    g.parse(ONTOLOGY_FILE, format="turtle")
    TRIPLE_BUDGET = MAX_TRIPLES - len(g)
    if TRIPLE_BUDGET <= 0:
        raise RuntimeError("Ontologie allein überschreitet das Budget.")
else:
    # Ohne Ontologie weiterarbeiten ist möglich, aber nicht empfohlen
    pass

# ------------------ 1) Titel-Seed aus basics ------------------
seed_titles: Set[str] = set()
series_titles: Set[str] = set()  # tconst von Serien
episode_titles: Set[str] = set() # tconst von Episoden

basics_path = os.path.join(IMDB_PATH, "title.basics.tsv.gz")
usecols = ["tconst","titleType","primaryTitle","originalTitle",
           "isAdult","startYear","endYear","runtimeMinutes","genres"]
for chunk in gz_read(basics_path, usecols=usecols, chunksize=200_000):
    if not budget_ok():
        break
    for _, row in chunk.iterrows():
        if not budget_ok():
            break
        tconst = norm_str(row.get("tconst"))
        if not tconst:
            continue
        if len(seed_titles) >= SEED_TITLES_MAX:
            break

        titleType = norm_str(row.get("titleType")) or ""
        t_iri = iri_title(tconst)

        # Typen setzen
        add_t(t_iri, RDF.type, IMD.Title)
        if titleType == "tvSeries":
            add_t(t_iri, RDF.type, IMD.TVSeries)
            series_titles.add(tconst)
        elif titleType == "tvEpisode":
            add_t(t_iri, RDF.type, IMD.Episode)
            episode_titles.add(tconst)

        add_t(t_iri, IMD.titleID, Literal(tconst, datatype=XSD.string))

        v = norm_str(row.get("primaryTitle"))
        if v: add_t(t_iri, IMD.primaryTitle, Literal(v, datatype=XSD.string))
        v = norm_str(row.get("originalTitle"))
        if v: add_t(t_iri, IMD.originalTitle, Literal(v, datatype=XSD.string))

        v = norm_str(row.get("isAdult"))
        if v:
            s = v.lower()
            b = True if s in {"1","true","t"} else False
            add_t(t_iri, IMD.isAdult, Literal(b, datatype=XSD.boolean))

        v = norm_str(row.get("startYear"))
        if v: add_t(t_iri, IMD.startYear, Literal(v, datatype=XSD.string))
        v = norm_str(row.get("endYear"))
        if v: add_t(t_iri, IMD.endYear, Literal(v, datatype=XSD.string))

        v = norm_str(row.get("runtimeMinutes"))
        if v and re.fullmatch(r"-?\d+", v):
            add_t(t_iri, IMD.runtimeMinutes, Literal(int(v), datatype=XSD.integer))

        v = titleType
        if v: add_t(t_iri, IMD.type, Literal(v, datatype=XSD.string))

        v = norm_str(row.get("genres"))
        if v:
            for g_ in str(v).split(","):
                gclean = g_.strip()
                if gclean:
                    add_t(t_iri, IMD.genre, Literal(gclean, datatype=XSD.string))

        seed_titles.add(tconst)
    if len(seed_titles) >= SEED_TITLES_MAX or not budget_ok():
        break

# ------------------ 2) Ratings zu Seed-Titeln ------------------
ratings_path = os.path.join(IMDB_PATH, "title.ratings.tsv.gz")
usecols = ["tconst","averageRating","numVotes"]
for chunk in gz_read(ratings_path, usecols=usecols, chunksize=200_000):
    if not budget_ok():
        break
    for _, row in chunk.iterrows():
        if not budget_ok():
            break
        tconst = norm_str(row.get("tconst"))
        if not tconst or tconst not in seed_titles:
            continue

        r_iri = URIRef(to_iri(f"{RES}rating/{tconst}"))
        add_t(r_iri, RDF.type, IMD.Rating)

        v = norm_str(row.get("averageRating"))
        if v:
            try:
                add_t(r_iri, IMD.averageRating, Literal(str(float(v)), datatype=XSD.decimal))
            except:
                pass
        v = norm_str(row.get("numVotes"))
        if v and re.fullmatch(r"-?\d+", v):
            add_t(r_iri, IMD.numVotes, Literal(int(v), datatype=XSD.integer))

        add_t(iri_title(tconst), IMD.hasRating, r_iri)

# ------------------ 3) AKAs (max. n pro Titel) ------------------
akas_path = os.path.join(IMDB_PATH, "title.akas.tsv.gz")
usecols = ["titleId","ordering","title","region","language"]
akas_count: Dict[str,int] = {}
for chunk in gz_read(akas_path, usecols=usecols, chunksize=200_000):
    if not budget_ok():
        break
    for _, row in chunk.iterrows():
        if not budget_ok():
            break
        titleId = norm_str(row.get("titleId"))
        if not titleId or titleId not in seed_titles:
            continue
        ordering = norm_str(row.get("ordering")) or "1"
        if akas_count.get(titleId, 0) >= AKAS_PER_TITLE_MAX:
            continue

        a_iri = iri_aka(titleId, ordering)
        add_t(a_iri, RDF.type, IMD.AlternateTitle)

        v = norm_str(row.get("title"))
        if v: add_t(a_iri, IMD.alternateTitle, Literal(v, datatype=XSD.string))
        v = norm_str(row.get("region"))
        if v: add_t(a_iri, IMD.region, Literal(v, datatype=XSD.string))
        v = norm_str(row.get("language"))
        if v: add_t(a_iri, IMD.language, Literal(v, datatype=XSD.string))

        add_t(iri_title(titleId), IMD.hasAlternateTitle, a_iri)
        akas_count[titleId] = akas_count.get(titleId, 0) + 1

# ------------------ 4) Episoden-Infos für einige Serien ------------------
episode_path = os.path.join(IMDB_PATH, "title.episode.tsv.gz")
usecols = ["tconst","parentTconst","seasonNumber","episodeNumber"]
episodes_per_series: Dict[str,int] = {}
for chunk in gz_read(episode_path, usecols=usecols, chunksize=200_000):
    if not budget_ok():
        break
    for _, row in chunk.iterrows():
        if not budget_ok():
            break
        tconst = norm_str(row.get("tconst"))
        parent = norm_str(row.get("parentTconst"))
        if not tconst or not parent:
            continue
        # nur Episoden aufnehmen, wenn die Serie im Seed ist oder das Budget es erlaubt
        if parent not in seed_titles:
            continue
        if episodes_per_series.get(parent, 0) >= EPISODES_PER_SERIES_MAX:
            continue

        t_iri = iri_title(tconst)
        add_t(t_iri, RDF.type, IMD.Episode)

        v = norm_str(row.get("seasonNumber"))
        if v and re.fullmatch(r"-?\d+", v):
            add_t(t_iri, IMD.seasonNumber, Literal(int(v), datatype=XSD.integer))
        v = norm_str(row.get("episodeNumber"))
        if v and re.fullmatch(r"-?\d+", v):
            add_t(t_iri, IMD.episodeNumber, Literal(int(v), datatype=XSD.integer))

        add_t(t_iri, IMD.parentSeries, iri_title(parent))
        seed_titles.add(tconst)  # Episode zählt jetzt als bekannter Titel
        episodes_per_series[parent] = episodes_per_series.get(parent, 0) + 1

# ------------------ 5) Rollen aus principals + crew ------------------
people_seen: Set[str] = set()

# 5a) principals
principals_path = os.path.join(IMDB_PATH, "title.principals.tsv.gz")
usecols = ["tconst","nconst","category","job"]
per_title_count: Dict[str,int] = {}
for chunk in gz_read(principals_path, usecols=usecols, chunksize=200_000):
    if not budget_ok():
        break
    for _, row in chunk.iterrows():
        if not budget_ok():
            break
        tconst = norm_str(row.get("tconst"))
        nconst = norm_str(row.get("nconst"))
        if not tconst or not nconst or tconst not in seed_titles:
            continue
        if per_title_count.get(tconst, 0) >= PRINCIPALS_PER_TITLE:
            continue

        cat = norm_str(row.get("category")) or ""
        if cat == "actress":
            cat = "actor"
        if not cat:
            job = norm_str(row.get("job"))
            cat = job or "role"

        person = iri_person(nconst)
        role = iri_role(tconst, cat, nconst)
        title = iri_title(tconst)

        add_t(role, RDF.type, IMD.Role)
        add_t(role, IMD.roleName, Literal(cat, datatype=XSD.string))
        add_t(person, IMD.hasRole, role)
        add_t(role, IMD.roleIn, title)

        people_seen.add(nconst)
        per_title_count[tconst] = per_title_count.get(tconst, 0) + 1

# 5b) crew (directors, writers)
crew_path = os.path.join(IMDB_PATH, "title.crew.tsv.gz")
usecols = ["tconst","directors","writers"]
per_title_crew: Dict[str,int] = {}
for chunk in gz_read(crew_path, usecols=usecols, chunksize=200_000):
    if not budget_ok():
        break
    for _, row in chunk.iterrows():
        if not budget_ok():
            break
        tconst = norm_str(row.get("tconst"))
        if not tconst or tconst not in seed_titles:
            continue
        if per_title_crew.get(tconst, 0) >= CREW_ROLES_PER_TITLE:
            continue

        def emit_list(field, role_name):
            global TRIPLE_BUDGET
            vals = norm_str(row.get(field))
            if not vals:
                return 0
            c = 0
            for n in vals.split(","):
                nconst = n.strip()
                if not nconst:
                    continue
                if per_title_crew.get(tconst, 0) >= CREW_ROLES_PER_TITLE:
                    break
                person = iri_person(nconst)
                role = iri_role(tconst, role_name, nconst)
                title = iri_title(tconst)

                add_t(role, RDF.type, IMD.Role)
                add_t(role, IMD.roleName, Literal(role_name, datatype=XSD.string))
                add_t(person, IMD.hasRole, role)
                add_t(role, IMD.roleIn, title)
                people_seen.add(nconst)
                per_title_crew[tconst] = per_title_crew.get(tconst, 0) + 1
                c += 1
            return c

        emit_list("directors", "director")
        emit_list("writers", "writer")

# ------------------ 6) Personenstammdaten ------------------
people_limit_left = max(0, PEOPLE_LIMIT - len(people_seen))
if people_limit_left > 0:
    name_path = os.path.join(IMDB_PATH, "name.basics.tsv.gz")
    usecols = ["nconst","primaryName","birthYear","deathYear",
               "primaryProfession","knownForTitles"]
    for chunk in gz_read(name_path, usecols=usecols, chunksize=200_000):
        if not budget_ok() or people_limit_left <= 0:
            break
        for _, row in chunk.iterrows():
            if not budget_ok() or people_limit_left <= 0:
                break
            nconst = norm_str(row.get("nconst"))
            if not nconst or nconst not in people_seen:
                continue

            p_iri = iri_person(nconst)
            add_t(p_iri, RDF.type, IMD.Person)
            add_t(p_iri, IMD.personID, Literal(nconst, datatype=XSD.string))

            v = norm_str(row.get("primaryName"))
            if v: add_t(p_iri, RDFS.label, Literal(v, datatype=XSD.string))
            v = norm_str(row.get("birthYear"))
            if v: add_t(p_iri, IMD.birthYear, Literal(v, datatype=XSD.string))
            v = norm_str(row.get("deathYear"))
            if v: add_t(p_iri, IMD.deathYear, Literal(v, datatype=XSD.string))

            prof = norm_str(row.get("primaryProfession")) or ""
            if "actress" in prof:
                add_t(p_iri, IMD.gender, Literal("female", datatype=XSD.string))
            elif "actor" in prof:
                add_t(p_iri, IMD.gender, Literal("male", datatype=XSD.string))

            kf = norm_str(row.get("knownForTitles"))
            if kf:
                for tt in kf.split(","):
                    tt_ = tt.strip()
                    if tt_:
                        add_t(p_iri, IMD.knownFor, iri_title(tt_))

            people_limit_left -= 1

# ------------------ 7) Speichern ------------------
g.serialize(destination=OUT_FILE, format="turtle")
print(f"[OK] Geschrieben: {OUT_FILE}  | Tripel: {len(g)}  | Restbudget: {TRIPLE_BUDGET}")
