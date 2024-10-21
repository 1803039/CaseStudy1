#!/usr/bin/python3

import socket
import threading
import time

# Table of (IP, PORT) tuples for each node (can add more nodes as needed)
PEERS = [
    ('127.0.0.1', 5001),  # Peer node 1
    ('127.0.0.1', 5002),  # Peer node 2
    ('127.0.0.1', 5003),  # Peer node 3
]

# Mapping types to peer IPs and Ports (specific instruction types to specific peers)
TYPE_TO_NODE = {
    'a': PEERS[0],  # 'a' tasks go to peer node 1
    'b': PEERS[1],  # 'b' tasks go to peer node 2
    'c': PEERS[2],  # 'c' tasks go to peer node 3
}

# Global variable to store discovered peers that responded with Dis_ACK
discovered_peers = []

# Helper function to send messages
def send_message(ip, port, message):
    try:
        client_socket = socket.socket()
        client_socket.connect((ip, port))
        client_socket.send(message.encode())
        response = client_socket.recv(1024).decode()
        print(f"Received response: {response} from {ip}:{port}")
        client_socket.close()
        return response
    except Exception as e:
        print(f"Error connecting to {ip}:{port}: {e}")
        return None

# Server logic to handle incoming messages
def handle_client_connection(conn, addr):
    message = conn.recv(1024).decode()
    print(f"Received message: {message} from {addr}")

    if message == "Discovery":
        print(f"Discovery received from {addr}, sending Dis_ACK")
        conn.send("Dis_ACK".encode())

    elif message == "Conn_Req":
        print(f"Connection Request received from {addr}, sending Conn_ACK")
        conn.send("Conn_ACK".encode())

    elif message.startswith("exec"):
        task = message.split(" ")[1]
        print(f"Executing task: {task}")
        time.sleep(1)  # Simulate task execution time
        conn.send(f"{task} is executed".encode())

    conn.close()

# Server program to listen for incoming connections
def server_program(ip, port):
    server_socket = socket.socket()
    server_socket.bind((ip, port))
    server_socket.listen(5)
    print(f"Server listening on {ip}:{port}")

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client_connection, args=(conn, addr)).start()

def start_server(ip, port):
    threading.Thread(target=server_program, args=(ip, port)).start()

# Client logic to discover peers, send connection requests, and send instructions
def discover_and_connect(ip, port, input_file):
    global discovered_peers

    # Step 1: Discovery
    print("Starting peer discovery...")
    for peer_ip, peer_port in PEERS:
        if (peer_ip, peer_port) != (ip, port):  # Avoid sending to self
            print(f"Sending Discovery to {peer_ip}:{peer_port}")
            response = send_message(peer_ip, peer_port, "Discovery")
            if response == "Dis_ACK":
                discovered_peers.append((peer_ip, peer_port))
                print(f"Discovered peer: {peer_ip}:{peer_port}")

    # Print active peers
    print("\nActive peers that responded with Dis_ACK:")
    for peer_ip, peer_port in discovered_peers:
        print(f"- {peer_ip}:{peer_port}")

    # Step 2: Send Conn_Req to discovered peers
    for peer_ip, peer_port in discovered_peers:
        print(f"Sending Conn_Req to {peer_ip}:{peer_port}")
        response = send_message(peer_ip, peer_port, "Conn_Req")
        if response == "Conn_ACK":
            print(f"Connected with peer: {peer_ip}:{peer_port}")
        else:
            print(f"Connection failed with {peer_ip}:{peer_port}")

    # Step 3: Send instructions from input file to specific peers
    with open(input_file, 'r') as file:
        instructions = file.read().strip().split()

    # Process and send each instruction to its corresponding peer (based on type)
    for instruction in instructions:
        task_type = instruction[0]
        node_ip, node_port = TYPE_TO_NODE.get(task_type, (None, None))

        if node_ip and (node_ip, node_port) in discovered_peers:
            print(f"Sending {instruction} to {node_ip}:{node_port}")
            response = send_message(node_ip, node_port, f"exec {instruction}")
            if response:
                print(f"Task {instruction} executed: {response}")
        else:
            print(f"No active peer found for task type '{task_type}'")

    print("All Execution Done")

# Main program to start P2P node
if __name__ == '__main__':
    # Assign unique IP and port for this node
    my_ip = '127.0.0.1'  # Example IP (adjust based on your setup)
    my_port = 5000        # Unique port for this node

    # Start the server in a separate thread
    start_server(my_ip, my_port)

    # Start peer discovery, connection, and instruction processing
    input_file = 'input.txt'  # Input file containing the task sequence
    discover_and_connect(my_ip, my_port, input_file)


