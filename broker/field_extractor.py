import re

def extract_field(log_entry) :
    if "process" in log_entry and "host" in log_entry:
        message = log_entry.get("message", "")  
        port_pattern = r'port (\d+)'
        match = re.search(port_pattern, message)

        if match:
            port = match.group(1)
            log_entry['srcport'] = port
            # print(f"Extracted Port: {port}")
        else:
            log_entry['srcport'] = None
        return log_entry
    
    elif "bytes" in log_entry and "packets" in log_entry:
        log_entry["message"] = log_entry.pop("log_status")
        return log_entry
    else :
        log_entry["srcport"] = "8080"
        return log_entry