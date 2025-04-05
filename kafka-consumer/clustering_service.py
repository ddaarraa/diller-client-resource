import numpy as np
import uuid
import logging
import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics.pairwise import cosine_similarity
from db import save_to_mongodb

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

    similarity_matrix = cosine_similarity(features)
    
    logging.info("Cosine Similarity Matrix:")
    logging.info(similarity_matrix)

    save_to_correlation_db(messages=messages, similarity_matrix=similarity_matrix)

def save_to_correlation_db(messages, similarity_matrix) :
    correlation_array = []

    log_ids = [msg.get("_id", f"no_id_{i}") for i, msg in enumerate(messages)]

    for i in range(len(messages)):
        for j in range(len(messages)):
            correlation_array.append({
                "log_id_x" + str(i): log_ids[i],
                "log_id_y" + str(j): log_ids[j],
                "value": round(float(similarity_matrix[i][j]), 4)
            })

    #  Generate new correlation ID (cor_id) every time
    cor_id = str(uuid.uuid4())

    correlation_output = {
        "_id": cor_id,
        "correlation": correlation_array
    }

    # print (correlation_output)
    logging.info("Correlation Output:")
    print(json.dumps(correlation_output, indent=2))

    save_to_mongodb(message=correlation_output, collection="correlation")

    



    


# **Example Logs**
# logs = [
#     {"message": "Accepted publickey for ec2-user", "srcport": 17487, "srcaddr": "27.55.94.25"},
#     {"message": "Accepted password for user1", "srcport": 17487, "srcaddr": "27.55.94.30"},
#     {"message": "Failed password for root", "srcport": 17487, "srcaddr": "27.55.94.34"},
#     {"message": "Failed password for sigma", "srcport": 17487, "srcaddr": "27.55.94.34"},
# ]

logs = [
    {
    "_id": "67dba2df54f0d7001029caca",
    "source": "application",
    "log": "Service started successfully",
    "container_id": "container_1",
    "container_name": "app-service",
    "srcaddr": "204.76.203.80",
    "method": "POST",
    "message": "Processing request",
    "status": "200",
    "action": "ACCEPT",
    "time": "2025-02-19 15:16:39"
},
{
    "_id": "67dc306a54f0d7001029cacb",
    "host": "ip-172-31-31-244",
    "process": "sshd[3976]",
    "message": "Accepted publickey for ec2-user from 27.55.94.25 port 17487 ssh2: RSA …",
    "srcaddr": "204.76.203.80",
    "action": "REJECT",
    "time": "2025-02-19 15:16:39"
},
{
    "_id": "67bcf76ceed12c00103fe808",
    "version": "2",
    "account_id": "897722688543",
    "interface_id": "eni-0b192e9393e30b07f",
    "srcaddr": "172.31.20.53",
    "dstaddr": "49.228.98.209",
    "srcport": "27017",
    "dstport": "57192",
    "protocol": "6",
    "packets": "6",
    "bytes": "2118",
    "end": "1739978230",
    "action": "ACCEPT",
    "log_status": "OK",
    "time": "2025-02-19 15:16:39"
},
{
    "_id": "67bcf76ceed12c00103fe807",
    "version": "2",
    "account_id": "897722688543",
    "interface_id": "eni-0b192e9393e30b07f",
    "srcaddr": "204.76.203.80",
    "dstaddr": "172.31.20.53",
    "srcport": "14421",
    "dstport": "123",
    "protocol": "17",
    "packets": "1",
    "bytes": "36",
    "end": "1739978230",
    "action": "REJECT",
    "log_status": "OK",
    "time": "2025-02-19 15:16:39"
}

]

calculate_log_correlation(logs) 

