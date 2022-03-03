import socket 
import sys
import threading
import os
import hashlib
import time
import math
import concurrent.futures

if len(sys.argv) != 3: raise Exception("Error: Please pass the server details <path to info of servers.txt> <server number> as command line argument")
file_servers = open(sys.argv[1], 'r')
server_lines = [line.split("###$$$@@@") for line in file_servers.readlines()]
file_servers.close()
LIST_SERVERS_PORT = [int(line[0]) for line in server_lines]; LIST_SERVERS_PORT.pop(int(sys.argv[2])-1)
SERVER_PORT_NUMBER, SERVER_PATH = server_lines[int(sys.argv[2])-1]
SERVER_PORT_NUMBER = int(SERVER_PORT_NUMBER)
if SERVER_PATH.endswith("\n"): SERVER_PATH = SERVER_PATH[:-1]
SERVER_IP_ADDRESS = socket.gethostbyname(socket.gethostname())
if len(SERVER_PATH) == 0 or SERVER_PATH[-1] != "/": raise Exception("Error: Please pass the <server path> that ends with /")
CHUNK_SIZE = 32 * (2**10)
print("Server details are: %s:%d" % (SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))

LOCKS_FILES = {} # True = free, False = not free
for file_name in os.listdir(SERVER_PATH):
    if not file_name.startswith("."):
        LOCKS_FILES[file_name] = True

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((SERVER_IP_ADDRESS, SERVER_PORT_NUMBER))
s.listen()

server_sync_delay = 10
class SyncThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        while True:
            time.sleep(server_sync_delay)
            for port_no in LIST_SERVERS_PORT:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((SERVER_IP_ADDRESS, port_no))
                except:
                    continue
                s.send(b'7')
                data_files = [file.split("\t") for file in s.recv(CHUNK_SIZE).decode('ascii').split("###$$$@@@")]
                s.close()
                for file_detail in data_files:
                    if file_detail == ['']: break
                    file_name, file_size, file_modified_time = file_detail
                    file_size = int(file_size); file_modified_time = float(file_modified_time)
                    if os.path.exists(SERVER_PATH + file_name):
                        file_metadata = open(SERVER_PATH + ".metadata-" + file_name, "r")
                        my_file_modified_time = float(file_metadata.readline()[:-1])
                        file_metadata.close()
                        if my_file_modified_time < file_modified_time:
                            # update
                            os.remove(SERVER_PATH + file_name)
                            os.remove(SERVER_PATH + ".metadata-" + file_name)
                        else:
                            # no need to do anything
                            continue
                    # download
                    LOCKS_FILES[file_name] = False
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((SERVER_IP_ADDRESS, port_no))
                    s.send(b'2')
                    s.send(file_name.encode('ascii'))
                    data = s.recv(1)
                    if data == b'1':
                        print("The file which you are trying to download does not exist on the server.")
                        continue
                    elif data != b'0': raise Exception("Error: Some error occurred in download data")
                    file_size = int(s.recv(32).decode('ascii'))
                    file_output = open(SERVER_PATH + file_name, "w")
                    file_output.seek(file_size-1)
                    file_output.write('\0')
                    file_output.close()
                    total_chunks = math.ceil(file_size / CHUNK_SIZE)
                    s.close()

                    with concurrent.futures.ThreadPoolExecutor(7) as thp:
                        thp.map(download_chunk, [(file_name, i, port_no) for i in range(total_chunks)])

                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((SERVER_IP_ADDRESS, port_no))
                    s.send(b'2')
                    s.send((".metadata-" + file_name).encode('ascii'))
                    data = s.recv(1)
                    if data == b'1':
                        print("The file which you are trying to download does not exist on the server.")
                        continue
                    elif data != b'0': raise Exception("Error: Some error occurred in download data")
                    file_size = int(s.recv(32).decode('ascii'))
                    file_output = open(SERVER_PATH + ".metadata-" + file_name, "w")
                    file_output.seek(file_size-1)
                    file_output.write('\0')
                    file_output.close()
                    total_chunks = math.ceil(file_size / CHUNK_SIZE)
                    s.close()

                    with concurrent.futures.ThreadPoolExecutor(7) as thp:
                        thp.map(download_chunk, [(".metadata-" + file_name, i, port_no) for i in range(total_chunks)])
                    
                    LOCKS_FILES[file_name] = True

def download_chunk(list_details):
    file_name, chunk_number, port_no = list_details
    file_output = open(SERVER_PATH + file_name, "rb+")
    file_output.seek(chunk_number * CHUNK_SIZE)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((SERVER_IP_ADDRESS, port_no))
    s.send(b'6')
    s.send((file_name + "###$$$@@@" + str(chunk_number)).encode('ascii'))
    data = s.recv(1)
    if data == b'1': 
        raise Exception("Error: File \"" + file_name + "\" not present at the server")
    file_output.write(s.recv(CHUNK_SIZE))
    file_output.close()
    s.close()

st = SyncThread()
st.start()

class ThreadTask(threading.Thread):
    def __init__(self, conn):
        threading.Thread.__init__(self)
        self.conn = conn
    def run(self):
        command = self.conn.recv(1)
        if command == b'1':
            file_name, file_size, file_modified_time = self.conn.recv(256).decode('ascii').split("###$$$@@@")
            file_size = int(file_size)
            if os.path.exists(SERVER_PATH + file_name):
                self.conn.send(b'1')
            else:
                self.upload_data(file_name, file_size, file_modified_time)
                self.conn.send(b'0')
        elif command == b'2':
            file_name = self.conn.recv(256).decode('ascii')
            if file_name.startswith(".metadata-"):
                key_name = file_name[10:]
            else:
                key_name = file_name
            if not os.path.exists(SERVER_PATH + file_name):
                self.conn.send(b'1')
            elif not LOCKS_FILES[key_name]:
                self.conn.send(b'2')
            else:
                self.conn.send(b'0')
                file_input = open(SERVER_PATH + file_name, "r")
                file_input.seek(0, os.SEEK_END)
                file_size = file_input.tell()
                file_input.close()
                self.conn.send(str(file_size).encode('ascii'))
        elif command == b'3':
            file_name, file_size, file_modified_time = self.conn.recv(256).decode('ascii').split("###$$$@@@")
            file_size = int(file_size)
            if not os.path.exists(SERVER_PATH + file_name):
                self.conn.send(b'1')
            else:
                self.update_data(file_name)
                self.upload_data(file_name, file_size, file_modified_time)
                self.conn.send(b'0')
        elif command == b'4':
            self.view_files()
        elif command == b'5':
            file_name, chunk_number = self.conn.recv(256).decode('ascii').split("###$$$@@@")
            chunk_number = int(chunk_number)
            if os.path.exists(SERVER_PATH + file_name):
                self.conn.send(b'0')
                self.upload_chunk(file_name, chunk_number)
            else:
                self.conn.send(b'1')
        elif command == b'6':
            file_name, chunk_number = self.conn.recv(256).decode('ascii').split("###$$$@@@")
            chunk_number = int(chunk_number)
            if os.path.exists(SERVER_PATH + file_name):
                self.conn.send(b'0')
                self.download_chunk(file_name, chunk_number)
            else:
                self.conn.send(b'1')
        elif command == b'7':
            self.sync_server()
        elif command == b'8':
            file_name = self.conn.recv(256).decode('ascii')
            LOCKS_FILES[file_name] = True
        self.conn.close()
    def update_data(self, file_name):
        LOCKS_FILES[file_name] = False
        os.remove(SERVER_PATH + file_name)
        os.remove(SERVER_PATH + ".metadata-" + file_name)
    def upload_data(self, file_name, file_size, file_modified_time):
        LOCKS_FILES[file_name] = False
        file_output = open(SERVER_PATH + file_name, "w")
        file_output.seek(file_size-1)
        file_output.write('\0')
        file_output.close()
        file_metadata = open(SERVER_PATH + ".metadata-" + file_name, "w")
        file_metadata.write(file_modified_time + "\n")
        file_metadata.close()
    def upload_chunk(self, file_name, chunk_number):
        file_input = open(SERVER_PATH + file_name, "rb+")
        file_input.seek(chunk_number * CHUNK_SIZE)
        data_chunk = self.conn.recv(CHUNK_SIZE)
        md_digest = hashlib.sha256(data_chunk).hexdigest()
        file_input.write(data_chunk)
        file_input.close()
        file_metadata = open(SERVER_PATH + ".metadata-" + file_name, "a")
        file_metadata.write(str(chunk_number) + " " + md_digest + "\n")
        file_metadata.close()
    def view_files(self):
        result = ""
        for file_name in os.listdir(SERVER_PATH):
            if not file_name.startswith(".") and LOCKS_FILES[file_name]:
                file_input = open(SERVER_PATH + file_name, "r")
                file_input.seek(0, os.SEEK_END)
                result += "\n" + file_name + "\t" + str(file_input.tell())
                file_input.close()
        self.conn.send(result.encode('ascii'))
    def download_chunk(self, file_name, chunk_number):
        file_output = open(SERVER_PATH + file_name, "rb")
        file_output.seek(chunk_number * CHUNK_SIZE)
        data_chunk = file_output.read(CHUNK_SIZE)
        self.conn.send(data_chunk)
        file_output.close()
    def sync_server(self):
        result = ""
        for file_name in os.listdir(SERVER_PATH):
            if not file_name.startswith(".") and LOCKS_FILES[file_name]:
                file_input = open(SERVER_PATH + file_name, "r")
                file_input.seek(0, os.SEEK_END)
                file_metadata = open(SERVER_PATH + ".metadata-" + file_name, "r")
                result += file_name + "\t" + str(file_input.tell()) + "\t" + file_metadata.readline()[:-1] + "###$$$@@@"
                file_input.close()
                file_metadata.close()
        self.conn.send(result.encode('ascii'))

while True:
    conn, addr = s.accept()
    t = ThreadTask(conn)
    t.start()

s.close()