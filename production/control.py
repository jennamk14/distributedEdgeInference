# Node Controller Script (control.py)

import socket
import json
import subprocess
import os
import signal
import time
import sys

# Configuration variables
MASTER_IP = '10.0.0.17'
MASTER_PORT = 1025
MY_IP = '10.0.0.17'
MY_PORT = 1026
RETRY_LIMIT = 5
RETRY_DELAY = 5  # seconds
running = {}  # Dictionary to keep track of running processes


def send_message_to_server(host=MY_IP, master_port=MASTER_PORT):
    """
    Sends a message to the master server with the status, IP, and port of the node controller.
    
    Args:
    status (str): The status of the node controller ('up' or 'down').
    host (str, optional): The hostname of the master server. Defaults to MASTER_IP.
    master_port (int, optional): The port number of the master server. Defaults to MASTER_PORT.
    my_ip (str): The IP of the node controller.
    my_port (int): The port number of the node controller.
    """
    message = {
        'status': "up",
        'ip': MY_IP,
        'port': MY_PORT
    }
    status="up"
    # Create a socket (SOCK_STREAM means a TCP socket)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Connect to server and send data
        sock.connect((host, master_port))
        sock.sendall(json.dumps(message).encode('utf-8'))
        print(f"Status '{status}' with IP {MY_IP} and port {MY_PORT} sent to {host}:{master_port}")


def listen_for_instructions(my_port):
    """Listen for instructions from the master server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((MY_IP, MY_PORT))
        server_socket.listen(1)
        print(f"Listening for instructions on port {my_port}")

        while True:
            print("Ready to accept a new connection.")
            connection, client_address = server_socket.accept()
            with connection:
                print(f"Connected to {client_address}")
                try:
                    while True:
                        data = connection.recv(1024).decode('utf-8')
                        if not data:
                            print("No more data from master, closing connection.")
                            break
                        instruction = json.loads(data)
                        print(f"Received instruction: {instruction}")
                        handle_instruction(connection, instruction)
                except Exception as e:
                    print(f"An error occurred while handling instructions: {e}")
                finally:
                    print("Closing the connection.")
                    connection.close()

def handle_instruction(connection, instruction):
    """Handle an individual instruction from the master server and send an acknowledgment."""
    try:
        task = instruction.get('task')
        message = instruction.get('message')
        print(f"Processing task: {task} with message: {message}")
        
        if task == 'bringup':
            args = message.split()
            proc = subprocess.Popen(['python3', 'server.py'] + args)
            running[message] = proc.pid
            print(f"Bringing up {message} with PID {proc.pid}")
        elif task == 'shutdown':
            pid = running.get(message)
            if pid:
                os.kill(pid, signal.SIGTERM)
                del running[message]
                print(f"Shut down {message} with PID {pid}")

        ack_message = json.dumps({'status': 'done'})
        print(f"Sending acknowledgment: {ack_message}")
        connection.sendall(ack_message.encode('utf-8'))
        print("Acknowledgment sent, waiting before closing connection.")
        time.sleep(1)  # Wait before closing to ensure the message is sent

    except Exception as e:
        print(f"Error handling instruction: {e}")
        error_message = json.dumps({'status': 'error', 'message': str(e)})
        print(f"Sending error acknowledgment: {error_message}")
        connection.sendall(error_message.encode('utf-8'))
        time.sleep(1)  # Small delay for the error message as well

def main():
    """Main function to set up node controller."""
    send_message_to_server()
    listen_for_instructions(MY_PORT)

if __name__ == "__main__":
    main()
