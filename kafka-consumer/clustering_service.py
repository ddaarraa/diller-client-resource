import json
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import logging
import numpy as np
from db import save_to_mongodb

logging.basicConfig(
    level=logging.INFO,  # Log only INFO and above
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]  # Logs to stdout
)
logger = logging.getLogger(__name__)

def extract_common_features(messages):
    """Extract only the common fields across all log formats."""
    features = []
    categorical_fields = ["srcaddr"]
    
    # Convert categorical fields to one-hot encoding
    categorical_data = [[msg.get(field, "UNKNOWN")] for msg in messages for field in categorical_fields]
    one_hot_encoder = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
    one_hot_encoded = one_hot_encoder.fit_transform(categorical_data)

    for i, msg in enumerate(messages):
        try:
            start_idx = i * len(categorical_fields)
            end_idx = start_idx + len(categorical_fields)
            one_hot_values = one_hot_encoded[start_idx:end_idx].flatten()

            # Use only one-hot values (since we are ignoring format-specific fields)
            features.append(one_hot_values)

        except ValueError as e:
            logger.error(f"Failed to process message: {msg} - Error: {e}")

    return np.array(features)


def cluster_messages_dbscan(messages):
    """Cluster messages using DBSCAN after feature extraction."""
    features = extract_common_features(messages)

    # Normalize features
    features = StandardScaler().fit_transform(features)

    # Apply DBSCAN
    dbscan_model = DBSCAN(eps=1.5, min_samples=1, metric="euclidean")  
    labels = dbscan_model.fit_predict(features)

    num_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    logging.info(f"Number of distinct clusters: {num_clusters}")

    clustered_messages = {}
    for label, message in zip(labels, messages):
        clustered_messages.setdefault(label, []).append(message)

    for cluster_id, cluster in clustered_messages.items():
        if cluster_id == -1:
            logging.info("\nNoise (unclustered messages):")
        else:
            logging.info(f"\nCluster {cluster_id}:")
        for msg in cluster:
            logging.info(msg)
            save_to_mongodb(msg)

