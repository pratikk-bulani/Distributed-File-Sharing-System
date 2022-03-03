import sys
import socket
import concurrent.futures
import os
import math

# Fetching the server details
if len(sys.argv) != 2: raise Exception("Error: Please pass the server details <ip address:port number> as command line argument")
SERVER_DETAILS = sys.argv[1].split(":")
if len(SERVER_DETAILS) != 2: raise Exception("Error: The format of server details is <ip address:port number> (passed as command line argument)")
SERVER_IP_ADDRESS, SERVER_PORT_NUMBER = SERVER_DETAILS; SERVER_PORT_NUMBER = int(SERVER_PORT_NUMBER)
del SERVER_DETAILS
CHUNK_SIZE = 32 * (2**10)

def upload_chunk(chunk_number):
    file_input = open(str_file_path, "rb")
    file_input.seek(chunk_number * CHUNK_SIZE)
    data_chunk = file_input.read(CHUNK_SIZE)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
    s.send(b'5')
    s.send((file_name + "###$$$@@@" + str(chunk_number)).encode('ascii'))
    data = s.recv(1)
    if data == b'1': 
        raise Exception("Error: File \"" + file_name + "\" not present at the server")
    s.send(data_chunk)
    file_input.close()
    s.close()

def download_chunk(chunk_number):
    file_output = open(str_file_path, "rb+")
    file_output.seek(chunk_number * CHUNK_SIZE)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
    s.send(b'6')
    s.send((file_name + "###$$$@@@" + str(chunk_number)).encode('ascii'))
    data = s.recv(1)
    if data == b'1': 
        raise Exception("Error: File \"" + file_name + "\" not present at the server")
    file_output.write(s.recv(CHUNK_SIZE))
    file_output.close()
    s.close()

while True:
    # Menu
    print("Menu", "1. Upload data", "2. Download data", "3. Update data", "4. View all files", "5. Exit", sep = "\n")
    choice = input("Enter your choice: ").strip()
    print("-----------------------------------------------------------------------------")

    if choice == "1":
        # upload data
        str_file_path = input("Enter the file path: ")
        if not os.path.isfile(str_file_path): raise Exception("Error: Please provide a file path")
        try:
            file_input = open(str_file_path, "r")
        except:
            raise Exception("Error: File not found")
        file_input.seek(0, os.SEEK_END)
        file_size = file_input.tell() # in bytes
        file_input.close()
        file_name = str_file_path.split("/")[-1]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
        s.send(b'1')
        s.send((file_name + "###$$$@@@" + str(file_size) + "###$$$@@@" + str(os.path.getmtime(str_file_path))).encode('ascii'))
        data = s.recv(1)
        if data == b'1':
            print("This file already exists in the server. Please use update data option to replace the file.")
            continue
        elif data != b'0': raise Exception("Error: Some error occurred in upload data")
        s.close()
        print("...Uploading started successfully...")
        total_chunks = math.ceil(file_size / CHUNK_SIZE)

        with concurrent.futures.ThreadPoolExecutor(7) as thp:
            thp.map(upload_chunk, list(range(total_chunks)))
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
        s.send(b'8')
        s.send(file_name.encode('ascii'))
        s.close()
    elif choice == "2":
        # download data
        str_file_path = input("Enter the file download path (include the file name): ")
        file_name = input("Enter the file name (in the server) that you need to download: ")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
        s.send(b'2')
        s.send(file_name.encode('ascii'))
        data = s.recv(1)
        if data == b'1':
            print("The file which you are trying to download does not exist on the server.")
            continue
        elif data == b'2':
            print("The file is currently locked. Please try again later")
            continue
        elif data != b'0': raise Exception("Error: Some error occurred in download data")
        file_size = int(s.recv(32).decode('ascii'))
        file_output = open(str_file_path, "w")
        file_output.seek(file_size-1)
        file_output.write('\0')
        file_output.close()
        print("...Downloading started successfully...")
        total_chunks = math.ceil(file_size / CHUNK_SIZE)
        s.close()
        with concurrent.futures.ThreadPoolExecutor(7) as thp:
            thp.map(download_chunk, list(range(total_chunks)))
    elif choice == "3":
        # update data
        str_file_path = input("Enter the file path: ")
        if not os.path.isfile(str_file_path): raise Exception("Error: Please provide a file path")
        try:
            file_input = open(str_file_path, "r")
        except:
            raise Exception("Error: File not found")
        file_input.seek(0, os.SEEK_END)
        file_size = file_input.tell() # in bytes
        file_input.close()
        file_name = str_file_path.split("/")[-1]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
        s.send(b'3')
        s.send((file_name + "###$$$@@@" + str(file_size) + "###$$$@@@" + str(os.path.getmtime(str_file_path))).encode('ascii'))
        data = s.recv(1)
        if data == b'1':
            print("The file which you are trying to update does not exist on the server.")
            continue
        elif data != b'0': raise Exception("Error: Some error occurred in update data")
        s.close()
        print("...Updating started successfully...")
        total_chunks = math.ceil(file_size / CHUNK_SIZE)

        with concurrent.futures.ThreadPoolExecutor(7) as thp:
            thp.map(upload_chunk, list(range(total_chunks)))
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
        s.send(b'8')
        s.send(file_name.encode('ascii'))
        s.close()
    elif choice == "4":
        # view all files
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
        s.send(b'4')
        print("FileName\tFileSize(bytes)", end = "")
        print(s.recv(CHUNK_SIZE).decode('ascii'))
        s.close()
    elif choice == "5":
        # exit
        break
    else:
        print("Error: Invalid choice entered")
    print("-----------------------------------------------------------------------------")
print("Thanks for using the service")