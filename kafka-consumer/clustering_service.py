import json
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import DBSCAN

def cluster_messages_dbscan(messages):
    # Convert JSON messages into plain text for vectorization
    text_messages = [json.dumps(msg) for msg in messages]

    # Convert messages into numerical features using CountVectorizer
    vectorizer = CountVectorizer()
    message_vectors = vectorizer.fit_transform(text_messages)

    # Perform clustering using DBSCAN (dynamic cluster count)
    dbscan_model = DBSCAN(eps=1.5, min_samples=1, metric='cosine')
    labels = dbscan_model.fit_predict(message_vectors)

    # Print number of distinct clusters
    num_clusters = len(set(labels)) - (1 if -1 in labels else 0)  # Exclude noise points if present
    print(f"Number of distinct clusters: {num_clusters}")

    # Group messages by cluster and print the clusters
    clustered_messages = {}
    for label, message in zip(labels, messages):
        clustered_messages.setdefault(label, []).append(message)

    for cluster_id, cluster in clustered_messages.items():
        if cluster_id == -1:
            print("\nNoise (unclustered messages):")
        else:
            print(f"\nCluster {cluster_id}:")
        for msg in cluster:
            print(msg)