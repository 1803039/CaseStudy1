#!/usr/bin/python3

import socket
import threading
import time

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

# Main entry point for the receiver node
if __name__ == '__main__':
    # You can set this to different IPs for each terminal
    my_ip = '127.0.0.1'  # Example IP (this will depend on the node)
    my_port = 5001        # Unique port for this instance (change based on node)

    # Start the server to listen for incoming connections
    server_program(my_ip, my_port)
    exit()

