import socket
import json
import threading
import time
import paramiko

#Configure before startup
MY_IP="10.0.0.17"
MY_CONTROLER_LISTEN_PORT=1025
MY_REQUEST_LISTEN=1027

# turn on/off model parallelism
MP = False

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
            sock.connect((host, port)) # Connect to the server
            sock.sendall(json.dumps(data).encode('utf-8')) # Send the JSON payload
            print(f"Sent to {host}:{port} - Task: {task}, Message: {message}") # Print a confirmation
        except ConnectionRefusedError:
            print(f"Connection to {host}:{port} refused. Make sure the server is running.") # Print an error
        except Exception as e:
            print(f"An error occurred: {e}") # Print an error

def handle_client_connection(connection, address):
    """
    Handles a single client connection. Receives data and sends a response.
    """
    with connection:
        print(f"Connected by {address}") # Print a confirmation
        try:
            data = connection.recv(1024) # Receive data from the client
            if data:
                json_data = json.loads(data.decode('utf-8')) # Decode the JSON data
                handle_node_status(json_data) # Handle the node status message
        except Exception as e:
            print(f"An error occurred with {address}: {e}")
        finally:
            connection.close()
            print(f"Connection with {address} closed.")

def handle_node_status(data):
    """
    Handles a node status message. Updates the node_controllers list.
    """

    status = data.get('status') # Get the status from the message
    ip = data.get('ip') # Get the IP address from the message
    port = data.get('port') # Get the port from the message

    with node_controllers_lock: # Acquire the lock
        node_tuple = (ip, port) # Create a tuple of the IP and port

        if status == 'up': # If the status is 'up'
            if node_tuple not in node_controllers: # If the node is not already in the list
                node_controllers.append(node_tuple)     # Add the node to the list
            print(f"Node controller added: {ip}:{port}") # Print a confirmation
        elif status == 'down': # If the status is 'down'
            if node_tuple in node_controllers: # If the node is in the list
                node_controllers.remove(node_tuple) # Remove the node from the list
            print(f"Node controller removed: {ip}:{port}") # Print a confirmation

def listen_for_connections(port):
    """
    Listens for incoming connections on the specified port. Spawns a thread to handle each connection.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket: # Create a socket
        server_socket.bind((MY_IP, port)) # Bind to the specified port
        server_socket.listen() # Listen for incoming connections
        print(f"Server is listening on localhost:{port}") # Print a confirmation

        while True: # Loop forever
            try: 
                connection, address = server_socket.accept() # Accept a connection
                client_thread = threading.Thread(target=handle_client_connection, args=(connection, address)) # Spawn a thread to handle the connection
                client_thread.daemon = True # Set the thread to close when the main thread closes
                client_thread.start() # Start the thread
            except Exception as e:
                print(f"An error occurred: {e}") # should we specify where the error occurred?

def start_listener_thread(port):
    """
    Starts a thread to listen for incoming connections on the specified port.
    """
    listener_thread = threading.Thread(target=listen_for_connections, args=(port,)) # Create a thread
    listener_thread.daemon = True # Set the thread to close when the main thread closes
    listener_thread.start() # Start the thread

############END NODE CONNECTION STUFF#############

#############REQUEST HANDLING STUFF 

# Define the global list to store the requests
requests_to_serve = []

# Function to handle client connections and receive data
def listen_for_requests(host, port):
    """
    Listens for incoming connections on the specified port. Spawns a thread to handle each connection.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket: # Create a socket
        server_socket.bind((host, port)) # Bind to the specified port
        server_socket.listen() # Listen for incoming connections
        print(f"Server listening on {host}:{port}") # Print a confirmation
        
        while True: # Loop forever
            conn, addr = server_socket.accept() # Accept a connection
            with conn: # Handle the connection in a with block
                while True: # Loop until the connection is closed
                    data = conn.recv(1024) # Receive data from the client
                    if not data: # If there is no data,
                        break # break out of the loop
                    try:    
                        # Decode the incoming data as JSON
                        message = json.loads(data.decode('utf-8'))
                        if all(k in message for k in ('ip', 'port', 'network')):
                            # Load the data into the global list as a tuple
                            global requests_to_serve # Use the global list
                            requests_to_serve.append((message['ip'], message['port'], message['network'])) # Add the request to the list
                            # Questions: does this 'network' mean the AI model to run? How to specify if whole model or just a layer?
                    except json.JSONDecodeError:
                        print("Received non-JSON data")

# Thread to listen for incoming dnn requests
def start_listen_for_requests_thread():
    """
    Starts a thread to listen for incoming dnn requests on the specified port.
    """
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
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock: # Create a socket
        try:
            sock.connect((host, port)) # Connect to the server
            sock.sendall(json.dumps(data).encode('utf-8')) # Send the JSON payload
            print(f"Sent to {host}:{port} - Entry_IP: {entry_ip}, Network: {network}") # Print a confirmation
        except ConnectionRefusedError: # If the connection is refused
            print(f"Connection to {host}:{port} refused. Make sure the server is running.")
        except Exception as e:
            print(f"An error occurred: {e}")


################################################## END REQUEST LOGIC###########

############################# RED EDGE LOGIC ###############################

# define goodness function for each available node controller given a request
def goodness_function(node, request):
    """
    Returns which node to run request on, based on goodness function.
    """
    
    # iterate over the node_controllers list to find the node controller with the best goodness score
    # where node is a tuple of (ip, port) of the node controller
    # where request is a tuple of (ip, port, network) of the request

    # goodness score is a function of the following:
    # 1. Memory utilization: which nodes have sufficient memory to store the model?
    # 2. CPU utilization: which node has the most cpu resources?

    # if model parallelism is turned on, then the mem and cpu requirements are divided by the number of layers in the model
    # if model parallelism is turned off, then the mem and cpu requirements are the same as the original mem and cpu requirements
    
    
    # get mem and cpu utilization of the node controller
    # save the mem and cpu utilization of the node controller as a tuple
    nodes = {}

    for node in node_controllers:
        mem_avail = get_node_memory_util(node)
        cpu_avail = get_node_cpu_util(node)
        node[node] = (mem_avail, cpu_avail)

    if MP:
        model_parallelism(request[2], nodes)
    else:
        # get the memory requirements of the whole model
        mem_req = get_model_mem_requirements(request[2])

        # get candidate nodes that have sufficient memory to run the model
        candidate_nodes = []
        for n in nodes:
            mem_avail = node[n][0]
            if mem_avail > mem_req:
                candidate_nodes.append(n)

        # return candidate node with the best cpu resources
        best_node = None
        best_cpu = 0
        for n in candidate_nodes:
            cpu_avail = node[n][1]
            if cpu_avail > best_cpu:
                best_node = n
                best_cpu = cpu_avail
        
        return best_node

def model_parallelism(model, nodes):
    """ 
    Returns the best node controller for each layer in the requested model (using model parallelism.)
    """
    # get pytorch object of the model
    model = get_model(model) # this should return the pytorch object of the model

    # get the layers in the model
    layers = [module for module in model.modules()] 

    # ASSUMPTIONS: 
    # sub-models are stored in the same node controller as the original model
    # the sub-set of layers can fit in the memory of the node controller

    # use hashing to determine which node controller to store each layer in
    # use largest (memory) and fastest (cpu) node controllers first

    # sort the node controllers by memory and cpu resources
    sorted_nodes = sorted(nodes, key=lambda x: (nodes[x][0], nodes[x][1]), reverse=True)

    # iterate over the layers in the model
    for layer in layers:
        # get the memory and cpu requirements of the layer
        mem_req = get_model_mem_requirements(layer)

        # get candidate nodes that have sufficient memory to run the layer
        candidate_nodes = []
        for n in sorted_nodes:
            mem_avail = nodes[n][0]
            if mem_avail > mem_req:
                candidate_nodes.append(n)

        # return candidate node with the best cpu resources
        best_node = None
        best_cpu = 0
        for n in candidate_nodes:
            cpu_avail = nodes[n][1]
            if cpu_avail > best_cpu:
                best_node = n
                best_cpu = cpu_avail
        
        # store the layer in the best node controller
        nodes[best_node][0] -= mem_req
        #nodes[best_node][1] -= cpu_req

        layer_node = (layer, best_node) # tuple of (layer, node controller) for each layer in the model

    # return the list of node controllers for each layer in the model
    return layer_node


def get_node_memory_util(node):
    """
    Returns the memory utilization of a node controller.
    """
    # get the memory usage of the node controller
    # where node is a tuple of (ip, port) of the node controller
    
    # ssh into the node controller
    # run the command 'free -m' and parse the output
    # return the memory utilization

    ssh = paramiko.SSHClient() # create ssh client
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # add the host key automatically
    ssh.connect(node[0], username='ubuntu', key_filename='/home/ubuntu/.ssh/id_rsa') # connect to the node controller
    stdin, stdout, stderr = ssh.exec_command('free -m') # run the command 'free -m'
    lines = stdout.readlines() # read the output
    ssh.close() # close the ssh client

    mem_total = int(lines[1].split()[1]) # get the total memory
    mem_used = int(lines[1].split()[2]) # get the used memory
    mem_avail = mem_total - mem_used # get the available memory
    return mem_avail

    # question for Austin: is there a better way to do this? I'm not sure if this is the best way to get the memory utilization

def get_node_cpu_util(node):
    """
    Returns the CPU utilization of a node controller.
    """
    # get the CPU usage of the node controller
    # where node is a tuple of (ip, port) of the node controller

    # ssh into the node controller
    # run the command 'top -b -n 1 | grep "Cpu(s)"' and parse the output
    # return the CPU utilization
    
    ssh = paramiko.SSHClient() # create ssh client
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # add the host key automatically
    ssh.connect(node[0], username='ubuntu', key_filename='/home/ubuntu/.ssh/id_rsa') # connect to the node controller
    stdin, stdout, stderr = ssh.exec_command('top -b -n 1 | grep "Cpu(s)"') # run the command 'top -b -n 1 | grep "Cpu(s)"'
    lines = stdout.readlines() # read the output
    ssh.close()

    cpu_util = float(lines[0].split()[1][:-1])  # get the CPU utilization
    cpu_total = float(lines[0].split()[3][:-1])  # get the total CPU
    cpu_avail = cpu_total - cpu_util # get the available CPU
    return cpu_avail

def get_model(model): # NEEDS WORK
    """
    Returns the pytorch object of a model.
    """
    # get the pytorch object of the model
    # where model is a string of the model name

    return model # question for austin: where are we storing the actual pytorch objects of the models?
 
def get_model_mem_requirements(model):
    """
    Returns the memory requirements of a model.
    """
    # get the memory requirements of the model
    # where model is a string of the model name

    # get pytorch object of the model
    model = get_model(model) # this should return the pytorch object of the model

    # get the memory requirements of the model to run inference only (not training)
    mem_params = sum([param.nelement()*param.element_size() for param in model.parameters()])
    mem_bufs = sum([buf.nelement()*buf.element_size() for buf in model.buffers()])
    mem = mem_params + mem_bufs # in bytes

    # return the memory requirements
    return mem

# def get_model_cpu_requirements(model): # NEEDS WORK (optional)
#     """
#     Returns the CPU requirements of a model.
#     """
#     # get the CPU requirements of the model
#     # where model is a string of the model name

#     # get pytorch object of the model
#     model = get_model(model) # this should return the pytorch object of the model

#     # get the CPU requirements of the model to run inference only (not training)
#     cpu_params = sum([param.nelement()*param.element_size() for param in model.parameters()])
#     cpu_bufs = sum([buf.nelement()*buf.element_size() for buf in model.buffers()])
#     cpu = cpu_params + cpu_bufs # in bytes

#     # return the CPU requirements
#    return cpu


def main_logic():
    """
    Main logic for the master.
    """

    while True:
        if(len(requests_to_serve)!=0): # If there are requests to serve
            ip, port, network = requests_to_serve.pop(0) # Pop the first request
            node = goodness_function(node_controllers, (ip, port, network)) # Get the best node controller for the request
            # TO DO: modify the goodness function to return a list of node controllers for each layer in the model
            send_task_message('bringup', "5000 True", node[0], node[1])
            reply_to_request(node[0], node[1], ip, port, network)
        with node_controllers_lock:
            ## Here you could iterate over the node_controllers list and send them tasks
            for host, port in node_controllers:
                print(node_controllers)
        print(requests_to_serve)
        time.sleep(10)

if __name__ == "__main__":
    start_listener_thread(MY_CONTROLER_LISTEN_PORT)
    start_listen_for_requests_thread()
    main_logic()  # Run the main logic of the master
