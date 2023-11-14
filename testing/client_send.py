import socket
import json
import sys
import time
MY_IP="10.0.0.17"
MY_PORT=1028

MASTER_IP="10.0.0.17"
MASTER_PORT=1027

def receive_message():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((MY_IP, MY_PORT))
        s.listen()
        print(f"Listening for messages on port {MY_PORT}")
        
        while(True):
            conn, _ = s.accept()
            with conn:
                data = conn.recv(1024)
                if data:
                    message = json.loads(data.decode())
                    print(f"Received entry ip: {message['entry_ip']}")
                    print(f"Received entry ip: {message['entry_port']}")
                    return((message['entry_ip'], message['entry_port'], message['network']))


def send_request():
    host = MASTER_IP # The server's hostname or IP address
    port = MASTER_PORT         # The port used by the server
    
    # Create a JSON payload with the required attributes
    request_data = json.dumps({
        'ip': '10.0.0.17',
        'port': '1028',
        'network': 'YOLO'
    }).encode('utf-8')
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        # Send the request
        client_socket.sendall(request_data)
        
        entry_data=receive_message()
        return entry_data


def send_message(server_ip, server_port, return_ip, return_port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, server_port))
        start_time = time.time()
        message_payload = {
            "message": message,
            "returnIP": return_ip,
            "returnPort": return_port,
            "start_time": start_time
        }
        s.sendall(json.dumps(message_payload).encode())

if __name__ == "__main__":

    data=send_request()
    server_ip = data[0]
    server_port = data[1]
    return_ip = MY_IP
    return_port = 5001

    time.sleep(5)

    send_message("10.0.0.17", 5000, "10.0.0.17", 5001, "M1-")
