import socket
import json
import threading
import time

#Configure before startup
MY_IP="10.0.0.17"
MY_CONTROLER_LISTEN_PORT=1025
MY_REQUEST_LISTEN=1027


#################NODE CONTROLLER CONNECTION STUFF#######
node_controllers = []  # List to keep track of node controllers
node_controllers_lock = threading.Lock()  # Lock for synchronizing access to the node_controllers list
request_to_serve=[]

def send_task_message(task, message, host, port):
    """
    Sends a JSON message with 'task' and 'message' attributes to the specified server.
    """
    data = {
        'task': task,
        'message': message
    }
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
            sock.sendall(json.dumps(data).encode('utf-8'))
            print(f"Sent to {host}:{port} - Task: {task}, Message: {message}")
        except ConnectionRefusedError:
            print(f"Connection to {host}:{port} refused. Make sure the server is running.")
        except Exception as e:
            print(f"An error occurred: {e}")

def handle_client_connection(connection, address):
    with connection:
        print(f"Connected by {address}")
        try:
            data = connection.recv(1024)
            if data:
                json_data = json.loads(data.decode('utf-8'))
                handle_node_status(json_data)
        except Exception as e:
            print(f"An error occurred with {address}: {e}")
        finally:
            connection.close()
            print(f"Connection with {address} closed.")

def handle_node_status(data):
    status = data.get('status')
    ip = data.get('ip')
    port = data.get('port')

    with node_controllers_lock:
        node_tuple = (ip, port)

        if status == 'up':
            if node_tuple not in node_controllers:
                node_controllers.append(node_tuple)
            print(f"Node controller added: {ip}:{port}")
        elif status == 'down':
            if node_tuple in node_controllers:
                node_controllers.remove(node_tuple)
            print(f"Node controller removed: {ip}:{port}")

def listen_for_connections(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((MY_IP, port))
        server_socket.listen()
        print(f"Server is listening on localhost:{port}")

        while True:
            try:
                connection, address = server_socket.accept()
                client_thread = threading.Thread(target=handle_client_connection, args=(connection, address))
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                print(f"An error occurred: {e}")

def start_listener_thread(port):
    listener_thread = threading.Thread(target=listen_for_connections, args=(port,))
    listener_thread.daemon = True
    listener_thread.start()

############END NODE CONNECTION STUFF#############

#############REQUEST HANDLING STUFF 

# Define the global list to store the requests
requests_to_serve = []

# Function to handle client connections and receive data
def listen_for_requests(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Server listening on {host}:{port}")
        
        while True:
            conn, addr = server_socket.accept()
            with conn:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    try:
                        # Decode the incoming data as JSON
                        message = json.loads(data.decode('utf-8'))
                        if all(k in message for k in ('ip', 'port', 'network')):
                            # Load the data into the global list as a tuple
                            global requests_to_serve
                            requests_to_serve.append((message['ip'], message['port'], message['network']))
                    except json.JSONDecodeError:
                        print("Received non-JSON data")

# Thread to listen for incoming dnn requests
def start_listen_for_requests_thread():
    listener_thread = threading.Thread(target=listen_for_requests, args=(MY_IP, MY_REQUEST_LISTEN))
    listener_thread.start()

#reply to requester with massage containing entry ip and port
def reply_to_request(entry_ip, entry_port, host, port, network):
    """
    entry ip and port are where the network the requester is looking for is running, 
    network is the dnn  requested, 
    host and port are the requesters info (where they should be listening)
    """
    data = {
        'entry_ip': entry_ip,
        'entry_port': entry_port,
        'network':network
    }
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
            sock.sendall(json.dumps(data).encode('utf-8'))
            print(f"Sent to {host}:{port} - Entry_IP: {entry_ip}, Network: {network}")
        except ConnectionRefusedError:
            print(f"Connection to {host}:{port} refused. Make sure the server is running.")
        except Exception as e:
            print(f"An error occurred: {e}")


##################################################END REQUEST LOGIC###########



def main_logic():
    while True:
        if(len(requests_to_serve)!=0):
            requests_to_serve.pop(0)
            send_task_message('bringup', "5000 True", "10.0.0.17", 1026)
            reply_to_request("10.0.0.17",5000, "10.0.0.17",1028, "YOLO")
        with node_controllers_lock:
            ## Here you could iterate over the node_controllers list and send them tasks
            for host, port in node_controllers:
                print(node_controllers)
        print(requests_to_serve)
        time.sleep(10)  # Adjust as necessary for your logic

if __name__ == "__main__":
    start_listener_thread(MY_CONTROLER_LISTEN_PORT)
    start_listen_for_requests_thread()
    main_logic()  # Run the main logic of the master
