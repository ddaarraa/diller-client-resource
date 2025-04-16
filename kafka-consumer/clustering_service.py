from datetime import datetime
import numpy as np
import uuid
import logging
import json
import pytz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from db import save_to_mongodb

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_common_fields(messages):
    common_fields = set(messages[0].keys())
    for msg in messages[1:]:
        common_fields &= set(msg.keys())
    
    common_fields = list(common_fields)

    # Exclude categorical fields from text_fields
    text_fields = [field for field in common_fields if field not in ['srcport', 'action']]

    # Prepare categorical data: srcport + action
    categorical_values = [
        [str(msg.get('srcport', 'unknown')), msg.get('action', 'UNKNOWN').upper()]
        for msg in messages
    ]

    encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
    categorical_features = encoder.fit_transform(categorical_values)

    # Process text fields with TF-IDF
    text_features = []
    for field in text_fields:
        texts = [msg.get(field, '') for msg in messages]
        if all(t.strip() == '' for t in texts):
            continue 
        try:
            vectorizer = TfidfVectorizer(stop_words='english', max_features=100)
            field_text_features = vectorizer.fit_transform(texts).toarray()
            if field_text_features.shape[1] > 0:
                text_features.append(field_text_features)
        except ValueError:
            continue

    text_features_combined = np.hstack(text_features) if text_features else np.array([])

    all_features = (
        np.hstack([categorical_features, text_features_combined])
        if text_features_combined.size else categorical_features
    )

    all_features = np.nan_to_num(all_features)

    return all_features

def calculate_log_correlation(messages):
    features = extract_common_fields(messages)
    similarity_matrix = cosine_similarity(features)
    save_to_correlation_db(messages=messages, similarity_matrix=similarity_matrix)


def save_to_correlation_db(messages, similarity_matrix) :

    correlation_array = []
    log_ids = [msg.get("_id", f"no_id_{i}") for i, msg in enumerate(messages)]
    for i in range(len(messages)):

        x_type = f"Sys {i + 1}"
        if 'container_id' in messages[i] :
            x_type = f"App {i + 1}"
        elif 'bytes' in messages[i] :
            x_type = f"Vpc {i + 1}"

        for j in range(len(messages)):

            y_type = f"Sys {j + 1}"
            if 'container_id' in messages[j] :
                y_type = f"App {j + 1}"
            elif 'bytes' in messages[j] :
                y_type =  f"VPC {j + 1}"

            correlation_array.append({
                "log_id_x": log_ids[i],
                "x_type" : x_type,
                "log_id_y": log_ids[j],
                "y_type" : y_type,
                "value": round(float(similarity_matrix[i][j]), 4)
            })

    cor_id = str(uuid.uuid4())
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    current_time_bangkok = datetime.now(bangkok_tz).isoformat()

    

    correlation_output = {
        "_id": cor_id,
        "date": current_time_bangkok,
        "correlation": correlation_array     
    }

    save_to_mongodb(message=correlation_output, collection="correlation")
