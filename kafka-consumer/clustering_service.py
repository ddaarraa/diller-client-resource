import numpy as np
import logging
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_common_fields(messages):
    """Extract only the common fields across all logs and process them accordingly."""
    # Find common fields
    common_fields = set(messages[0].keys())
    for msg in messages[1:]:
        common_fields &= set(msg.keys())  # Intersect fields to find common ones
    
    common_fields = list(common_fields)

    # Separate numeric and text fields
    numeric_fields = ['srcport']  # Only numeric field
    text_fields = [field for field in common_fields if field not in numeric_fields]

    # Extract numeric features
    numeric_features_data = np.array([[msg.get('srcport', 0)] for msg in messages])  # Default to 0 if missing
    numeric_features = StandardScaler().fit_transform(numeric_features_data)  # Normalize

    # Extract text features (TF-IDF for message, One-Hot for categorical fields)
    text_features = []
    for field in text_fields:
        texts = [msg.get(field, '') for msg in messages]
        vectorizer = TfidfVectorizer(stop_words='english', max_features=100)
        field_text_features = vectorizer.fit_transform(texts).toarray()
        text_features.append(field_text_features)

    # Combine text features
    text_features_combined = np.hstack(text_features) if text_features else np.array([])

    # Combine numeric + text features
    all_features = np.hstack([numeric_features, text_features_combined]) if text_features_combined.size else numeric_features
    return all_features


def calculate_log_correlation(messages):
    """Calculate correlation between entire logs."""
    features = extract_common_fields(messages)

    # # Pearson Correlation
    # correlation_matrix = np.corrcoef(features, rowvar=False)

    # Cosine Similarity
    similarity_matrix = cosine_similarity(features)

    # logging.info("Pearson Correlation Matrix:")
    # logging.info(correlation_matrix)

    logging.info("Cosine Similarity Matrix:")
    logging.info(similarity_matrix)


# **Example Logs**
# logs = [
#     {"message": "Accepted publickey for ec2-user", "srcport": 17487, "srcaddr": "27.55.94.25"},
#     {"message": "Accepted password for user1", "srcport": 17487, "srcaddr": "27.55.94.30"},
#     {"message": "Failed password for root", "srcport": 17487, "srcaddr": "27.55.94.35"},
# ]

# calculate_log_correlation(logs)