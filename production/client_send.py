import socket
import json
import sys
import time
import numpy as np
import matplotlib.pyplot as plt # optional

MY_IP="10.0.0.17"
MY_PORT=1028

MASTER_IP="10.0.0.17"
MASTER_PORT=1027

def receive_message(): 
    """
    Receives a message from a client and prints it.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # Create a socket
        s.bind((MY_IP, MY_PORT)) # Bind the socket to the port
        s.listen() # Listen for connections
        print(f"Listening for messages on port {MY_PORT}") # Print a confirmation
        
        while(True): # Loop forever
            conn, _ = s.accept() # Accept a connection
            with conn: # Handle the connection in a with block
                data = conn.recv(1024) # Receive data from the client
                if data: # If there is data,
                    message = json.loads(data.decode()) # Decode the data as JSON
                    print(f"Received entry ip: {message['entry_ip']}") # Print the data
                    print(f"Received entry ip: {message['entry_port']}") # Print the data
                    return((message['entry_ip'], message['entry_port'], message['network'])) # Print the data


def send_request():
    """
    Sends a request to the master node to add this node to the list of nodes serving requests.
    """
    host = MASTER_IP # The server's hostname or IP address
    port = MASTER_PORT         # The port used by the server
    
    # Create a JSON payload with the required attributes
    request_data = json.dumps({
        'ip': '10.0.0.17',
        'port': '1028',
        'network': 'YOLO'
    }).encode('utf-8')
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket: # Create a socket
        client_socket.connect((host, port)) # Connect to the server
        # Send the request
        client_socket.sendall(request_data)
        
        entry_data=receive_message() # Receive the response
        return entry_data # Return the response


def send_message(server_ip, server_port, return_ip, return_port, message):
    """
    Sends a message to the specified server.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # Create a socket
        s.connect((server_ip, server_port)) # Connect to the server
        start_time = time.time() # Get the current time
        message_payload = {
            "message": message,
            "returnIP": return_ip,
            "returnPort": return_port,
            "start_time": start_time
        }
        s.sendall(json.dumps(message_payload).encode())

def poisson_process_with_rate_variation(total_time, rates, change_points): # Jenna's code
    """
    Generate bursty arrival times using a Poisson process with rate variation.

    Parameters:
    - total_time: Total simulation time.
    - rates: List of rates corresponding to different time intervals.
    - change_points: List of time points where the rate changes.

    Returns:
    - arrival_times: List of arrival times for a particular model.
    """

    arrival_times = []
    current_time = 0

    for i in range(len(change_points)):
        rate = rates[i]
        duration = change_points[i] - current_time

        # Generate arrivals based on Poisson process
        arrivals = np.random.poisson(rate * duration)
        arrival_times.extend(np.sort(np.random.uniform(current_time, current_time + duration, arrivals)))
        current_time = change_points[i]

    # Handle the last interval
    rate = rates[-1]
    duration = total_time - current_time
    arrivals = np.random.poisson(rate * duration)
    arrival_times.extend(np.sort(np.random.uniform(current_time, current_time + duration, arrivals)))

    return np.array(arrival_times)

def generate_bursty_arrival_times():
    """
    Generate bursty arrival times using a Poisson process with rate variation for each model.
    Ask user for input for total_time, rates, and change_points.
    Note: we can also hardcode these values if we want.
    """
    # create dictionary to store arrival times for each model
    arrival_times_dict = {}

    total_time = input("Enter total simulation time: ") # example: 150 seconds
    # model_parallelism = input("Execute model parallelism (Y/N): ") # example: "Y" for yes
    model_count = input("Enter number of models: ") # example: 1
    for i in range(model_count):
        model = input("Enter model: ") # example: "M1"
        rates = input("Enter list of rates corresponding to different time intervals: ") # example: [0.5, 1, 0.2]
        change_points = input("Enter list of time points where the rate changes: ") # example: [50, 100]
        arrival_times = poisson_process_with_rate_variation(total_time, rates, change_points) # generate arrival times for each model
        arrival_times_dict[model] = arrival_times

    # convert dictionary to tuple of (model, arrival_times) pairs
    arrival_times = tuple(arrival_times_dict.items()), model_parallelism

    return arrival_times

if __name__ == "__main__":

    data=send_request()
    server_ip = data[0]
    server_port = data[1]
    return_ip = MY_IP
    return_port = 5001

    time.sleep(5)

    # get constants to generate bursty arrival times from user
    arrival_times, model_parallelism = generate_bursty_arrival_times()


    # send messages to server based on arrival times
    for a in arrival_times: # iterate through arrival times for each model
        model = a[0] # model
        arrival_times = a[1] # arrival times for each model
        for arrival_time in arrival_times: # iterate through arrival times for each model
            time.sleep(arrival_time) # wait until arrival time
            send_message(server_ip, server_port, return_ip, return_port, model)  # send_message("10.0.0.17", 5000, "10.0.0.17", 5001, "M1-")
            # question for Austin: where is this being sent to? is it being sent to the server or the next node?