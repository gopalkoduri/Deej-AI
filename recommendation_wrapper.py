import os

os.environ["env"] = "dev"
os.environ["AWS_PROFILE"] = "riyaz"

from db_utils import exec_db_command
import psycopg2

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

mb_conn = psycopg2.connect(
    dbname="musicbrainz_db",
    user="developer",
    password="Jfpr0Y779vTLvEpY8tvj",
    host="192.168.0.156",
    port="5433",
)
mb_cursor = mb_conn.cursor()


# Get the title, artist
def get_artist_titles(spotify_track_uris):
    query = """select track_uri, artist_name, track_name from dataset_smp_tracks where track_uri = ANY(%s)"""
    data = exec_db_command(
        query_string=query, args=(spotify_track_uris,), fetch_results=True
    )
    track_artist_title_map = {}
    if data and data[0]:
        for row in data:
            track_artist_title_map[row[0]] = {"artist": row[1], "title": row[2]}
    return track_artist_title_map


# Get the tags
def get_tags(title, artist):
    query = """SELECT tags FROM musicbrainz.recording_artist_tags
    WHERE fts_vector @@ plainto_tsquery('english', %s) limit 1;;
    """
    mb_cursor.execute(query, (artist + " " + title,))
    data = mb_cursor.fetchall()
    if data and data[0] and data[0][0]:
        return data[0][0]
    else:
        return "unknown"


# Function to find similar items
def find_similar(tfidf_matrix, index, top_n=5):
    cosine_similarities = cosine_similarity(
        tfidf_matrix[index : index + 1], tfidf_matrix
    ).flatten()
    related_docs_indices = [
        i for i in cosine_similarities.argsort()[::-1] if i != index
    ]
    return [(index, cosine_similarities[index]) for index in related_docs_indices][
        0:top_n
    ]


def order_by_similarity(input_set, number_of_recos=15):
    input_set = [f"spotify:track:{id}" for id in input_set]
    track_artist_title_map = get_artist_titles(input_set)

    # tags
    documents = []
    for rec_id in input_set:
        tags = get_tags(
            track_artist_title_map[rec_id]["title"],
            track_artist_title_map[rec_id]["artist"],
        )
        documents.append(tags)

    # Convert tags into TF-IDF features
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(documents)
    similar_items = find_similar(tfidf_matrix, 0, top_n=10)
    return [
        input_set[i[0]].replace("spotify:track:", "")
        for i in similar_items[:number_of_recos]
    ]


if __name__ == "__main__":
    input_set = """72jCZdH0Lhg93z6Z4hBjgj
	4sDRcUthqGUgVPFh6HIftR
	2kIq7Oq6YNfdUuTrmPC4w1
	4Mf6w0cowyBNcu2aoVC9GX
	2a8buvY3ETYzcNNzMDx17H
	3kfHdr2sYF2EeWEmBHquVj
	0qQrpYn7jdfiwSvbevwW0o
	6WxqW2asFcea4D4xfIiHTW
	5bPiULQ8j6EpEdEXisn7yi
	66UVpCZ5aH3VV3Ic3PBUrP
	7xR0fkFtH0551GhxBzyVpE
	7oQ09pJGn75oy9MIvkINWi
	7DVvm94B6KDKtI6wzG4Vvm
	31LEJN0CEvMzEsVqZo4Zl9
	7FiVh9NnYEgH67oE3jxxmX
	1bjFRZio15WLDDQy75JY2r""".split()
    order_by_similarity(input_set)
