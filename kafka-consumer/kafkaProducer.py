import json
# from kafka import KafkaProducer
import os 
from pymongo import MongoClient



MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpassword@localhost:27017/?directConnection=true&serverSelectionTimeoutMS=2000&authSource=admin&appName=mongosh+2.4.0")
client = MongoClient(MONGO_URI)
db = client["logs_db"]

# # Kafka Producer Setup
# producer = KafkaProducer(
#     bootstrap_servers="localhost:9093",
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# Fixed Log Data

network_logs = [
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

system_logs = [
    {
        "_id": "67dc306a54f0d7001029cacb",
        "host": "ip-172-31-31-244",
        "process": "sshd[3976]",
        "message": "Accepted publickey for ec2-user from 27.55.94.25 port 17487 ssh2: RSA …",
        "srcaddr": "204.76.203.80",
        "action": "REJECT",
        "time": "2025-02-19 15:16:39"
    }
]

application_logs = [
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
    }
]

db["sys_logs_collection"].delete_many(filter={})
db["application_logs_collection"].delete_many({})
db["vpc_logs_collection"].delete_many({})
db["sys_logs_collection"].insert_many(system_logs)
db["application_logs_collection"].insert_many(application_logs)
db["vpc_logs_collection"].insert_many(network_logs)

print("Fixed logs sent to DB!")