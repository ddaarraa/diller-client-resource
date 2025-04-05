from clustering_service import calculate_log_correlation

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
    "_id": "67dc306a54f0d7001029cace",
    "host": "ip-172-31-31-244",
    "process": "sshd[3976]",
    "message": "Accepted publickey for ec2-user from 27.55.94.25 port 17487 ssh2: RSA …",
    "srcaddr": "204.76.203.80",
    "action": "REJECT",
    "time": "2025-02-19 15:16:39"
},
{
    "_id": "67dc306a54f0d7001029cac2",
    "host": "ip-172-31-31-244",
    "process": "sshd[3976]",
    "message": "Accepted publickey for ec2-user from 27.55.94.25 port 17487 ssh2: RSA …",
    "srcaddr": "204.76.203.80",
    "action": "REJECT",
    "time": "2025-02-19 15:16:39"
},
{
    "_id": "67dc306a54f0d7001029cac1",
    "host": "ip-172-31-31-244",
    "process": "sshd[3976]",
    "message": "Accepted publickey for ec2-user from 27.55.94.25 port 17487 ssh2: RSA …",
    "srcaddr": "204.76.203.80",
    "action": "REJECT",
    "time": "2025-02-19 15:16:39"
},
{
    "_id": "67dc306a54f0d7001029cac5",
    "host": "ip-172-31-31-244",
    "process": "sshd[3976]",
    "message": "Accepted publickey for ec2-user from 27.55.94.25 port 17487 ssh2: RSA …",
    "srcaddr": "204.76.203.80",
    "action": "REJECT",
    "time": "2025-02-19 15:16:39"
},

]

calculate_log_correlation(logs) 
