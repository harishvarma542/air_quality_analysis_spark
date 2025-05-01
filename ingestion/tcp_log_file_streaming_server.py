import os
import shutil
import time
import csv
import socket

# TCP Configuration
TCP_HOST = 'localhost'
TCP_PORT = 9999

# Folder Paths
LOG_FOLDER_PATH = '/workspaces/air_quality_analysis_spark/ingestion/data/pending'
PROCESSED_FOLDER_PATH = os.path.join('/workspaces/air_quality_analysis_spark/ingestion/data/', 'processed')

def create_tcp_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((TCP_HOST, TCP_PORT))
    server_socket.listen(5)
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}...")
    return server_socket

def ensure_processed_folder():
    os.makedirs(PROCESSED_FOLDER_PATH, exist_ok=True)

def send_line_to_client(client_socket, line):
    client_socket.send((line + "\n").encode('utf-8'))
    print(f"Sent: {line}")

def process_csv_line(line):
    csv_reader = csv.reader([line])
    for record in csv_reader:
        return record
    return []

def process_log_file(file_path, client_socket):
    filename = os.path.basename(file_path)
    print(f"📄 Processing: {filename}")

    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    record = process_csv_line(line)

                    if record:
                        send_line_to_client(client_socket, ",".join(record))
                    else:
                        print(f"Skipping invalid line: {line}")

                    time.sleep(0.01)

    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return

    dest_path = os.path.join(PROCESSED_FOLDER_PATH, filename)
    shutil.move(file_path, dest_path)
    print(f"Moved {filename} to 'processed/'")

def accept_client_connections(server_socket):
    try:
        while True:
            print("Waiting for new client...")
            client_socket, client_address = server_socket.accept()
            print(f"New client connected: {client_address}")

            for filename in sorted(os.listdir(LOG_FOLDER_PATH)):
                file_path = os.path.join(LOG_FOLDER_PATH, filename)
                if not os.path.isfile(file_path) or filename == 'processed':
                    continue
                process_log_file(file_path, client_socket)

            client_socket.close()
            print(f"Closed connection with client: {client_address}")

    except Exception as e:
        print(f"Error accepting client connection: {e}")
    finally:
        server_socket.close()

def start_server():
    ensure_processed_folder()
    server_socket = create_tcp_server()
    accept_client_connections(server_socket)

if __name__ == '__main__':
    start_server()