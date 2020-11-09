import threading
import socket
import time
import json
import random
import uuid
import os
import shutil
import traceback

from introducer import *
from allToall import *
from gossip import *
from client import *
from master import *
import consts

host_address_dict = {
    "fa20-cs425-g16-01" : "172.22.156.52",
    "fa20-cs425-g16-02" : "172.22.158.52",
    "fa20-cs425-g16-03" : "172.22.94.52",
    "fa20-cs425-g16-04" : "172.22.156.53",
    "fa20-cs425-g16-05" : "172.22.158.53",
    "fa20-cs425-g16-06" : "172.22.94.53",
    "fa20-cs425-g16-07" : "172.22.156.54",
    "fa20-cs425-g16-08" : "172.22.158.54",
    "fa20-cs425-g16-09" : "172.22.94.54",
    "fa20-cs425-g16-10" : "172.22.156.55"
}

address_host_dict = {
    "172.22.156.52": "fa20-cs425-g16-01",
    "172.22.158.52": "fa20-cs425-g16-02",
    "172.22.94.52": "fa20-cs425-g16-03",
    "172.22.156.53": "fa20-cs425-g16-04",
    "172.22.158.53": "fa20-cs425-g16-05",
    "172.22.94.53": "fa20-cs425-g16-06",
    "172.22.156.54":  "fa20-cs425-g16-07",
    "172.22.158.54": "fa20-cs425-g16-08",
    "172.22.94.54": "fa20-cs425-g16-09",
    "172.22.156.55": "fa20-cs425-g16-10"
}


# Membership List:
# Id	ip_addr		port number	 heartbeat_count	unix_time   msg_protocol_type
membership_dict = {}

failed_nodes = set()

count = 0

if os.path.isdir('./sdfs'):
    shutil.rmtree('./sdfs',)
os.mkdir('./sdfs')

def print_ml():
    """
    Print Membership List to the terminal.
    :return: None
    """
    global count
    count += 1
    if count % 8 != 0:
        return
    self_hostname = socket.gethostname()
    consts.packets_logger.info('\n=== MembershipList of %s ===' % self_hostname)
    for key, value in membership_dict.items():
        consts.packets_logger.info('%s: %s %s %d' % (key, address_host_dict[value["ip_addr"]],  value["unix_time"], value["heartbeat_count"]))
    consts.packets_logger.info('============================')

def senderHandler():
    """
    Packet sender handler

    :return: None
    """
    
    current_protocol_type = None
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    while True:

        # Need to get introduced first so send a first sentto to introducer to get inroduced (sent in the else statement)
        if (not consts.HAVE_BEEN_INTRODUCED):
            time.sleep(1)
            continue

        # Got introduced, now start sending pings to other nodes
        consts.protocol_type_lock.acquire()
        current_protocol_type = consts.PROTOCOL_TYPE
        consts.protocol_type_lock.release()

        # Decide whether to send pings to gossip or all-to-all
        consts.membership_dict_lock.acquire()
        if current_protocol_type == 'GOSSIP':
            gossipSenderHandler(sock, consts.SELF_UNIQUE_ID, membership_dict)
        else:
            allToAllSenderHandler(sock, consts.SELF_UNIQUE_ID, membership_dict)
        consts.membership_dict_lock.release()
        # Make thread go to sleep for some time before start sending again
        time.sleep(consts.TIME_INTERVAL_GOSSIP)
    
    sock.close()


def receiverHandler():
    """
    Packet receiver handler

    :return: None
    """
    while True: 
        # Need to get introduced first (so send a first sentto to introducer to get inroduced)
        if (not consts.HAVE_BEEN_INTRODUCED):
            time.sleep(1)
            continue
        else:
            break

    # Set up the UDP sockets
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = (consts.SELF_IP, consts.DEFAULT_PORT)
    sock.bind(server_address)

    current_protocol_type = None

    while True:
        
        # Need to get introduced first (so send a first sentto to introducer to get inroduced)
        if (not consts.HAVE_BEEN_INTRODUCED):
            time.sleep(1)
            continue

        # after getting introduced, start receiving periodic signals from the sender
        consts.protocol_type_lock.acquire()
        current_protocol_type = consts.PROTOCOL_TYPE
        consts.protocol_type_lock.release()
        
        if current_protocol_type == 'GOSSIP':
            try:
                gossipReceiverHandler(sock, membership_dict)
            except Exception as e:
                if consts.membership_dict_lock.locked():
                    consts.membership_dict_lock.release()
                
                if consts.contention_to_be_failed_lock.locked():
                    consts.contention_to_be_failed_lock.release()
                
                if consts.file_table_lock.locked():
                    consts.file_table_lock.release()
                
                if consts.gossip_sender_queue_lock.locked():
                    consts.gossip_sender_queue_lock.release()
                
                if consts.requested_files_queue_lock.locked():
                    consts.requested_files_queue_lock.release()
                
                if consts.num_packets_lock.locked():
                    consts.num_packets_lock.release()

        else:
            allToAllReceiverHandler(sock, membership_dict)
    sock.close()

def protocolCheckerHandler():
    """
    Protocol Check handler

    :return: None
    """
    while True:
        if (not consts.HAVE_BEEN_INTRODUCED):
            time.sleep(1)
            continue
        
        # after getting introduced, see as to whether the protocol has changed
        consts.protocol_type_lock.acquire()
        with open("protocol_type.txt", "r") as f:
            type_protocol = f.readline()
        consts.PROTOCOL_TYPE = type_protocol
        consts.protocol_type_lock.release()
        time.sleep(consts.PROTOCOL_CHECKER_TIME)

def failCheckHandler():
    """
    Fail Check handler

    :return: None
    """
    current_protocol_type = None
    time.sleep(2)
    while True:
        consts.membership_dict_lock.acquire()
        print_ml()
        consts.membership_dict_lock.release()

        # Wait for the introducer to get intorduced
        if (not consts.HAVE_BEEN_INTRODUCED):
            time.sleep(1)
            continue

        consts.protocol_type_lock.acquire()
        current_protocol_type = consts.PROTOCOL_TYPE
        consts.protocol_type_lock.release()

        # decide based on ENV what protocol to follow
        consts.membership_dict_lock.acquire()
        if current_protocol_type == 'GOSSIP':
            gossipFailCheckHandler(membership_dict)
        else:
            allToAllFailCheckHandler(membership_dict)
        
        consts.membership_dict_lock.release()

        # Check if file requested has timmed out
        consts.requested_files_queue_lock.acquire()
    
        for address in consts.REQUESTED_FILES_QUEUE:
            if (time.time() - consts.REQUESTED_FILES_QUEUE[address]["request_time"]) > consts.TIMEOUT_FOR_FILE_REQUEST:
                data = consts.REQUESTED_FILES_QUEUE[address]
                replica_nodes = data["replica_nodes"]
                sdfs_file_name = data["sdfs_file_name"]
                local_file_name = data["local_file_name"]
                file_size = data["file_size"]
                
                consts.REQUESTED_FILES_QUEUE.pop(address)
                consts.membership_dict_lock.acquire()
                ask_random_replica_node_for_file(sdfs_file_name, local_file_name, file_size, replica_nodes, membership_dict)
                consts.membership_dict_lock.release()

        consts.requested_files_queue_lock.release()
        time.sleep(consts.MEMBERSHIP_FAIL_CHECK_TIME)

def leaveMembershipHandler():
    """
    Leave Membership handler

    :return: None
    """
    current_protocol_type = None
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    #Volunarily leave the membership cluster
    while True:
        consts.LEAVE_MEMBERSHIP = os.environ.get('LEAVE_MEMBERSHIP')
        if consts.LEAVE_MEMBERSHIP == "YES":
            consts.logger.info("Bye!! " + ip_address + " is voluntarily leaving the group!!")
            send_data = json.dumps({
                "id" : consts.SELF_UNIQUE_ID,
                "leaving": "YES"
            })

            #broadcast the Bye message to all neighbors and then force exit from the program
            consts.membership_dict_lock.acquire()
            for ID in membership_dict:
                sock.sendto(send_data.encode('utf-8'), (membership_dict[ID]["ip_addr"], consts.DEFAULT_PORT))    
            consts.membership_dict_lock.release()
            exit()
        else:
            time.sleep(consts.LEAVE_MEMBERSHIP_TIME)


def fileTransferHandler():
    while True:
        # device's IP address
        SERVER_HOST = consts.SELF_IP
        SERVER_PORT = 5001
        # receive 4096 bytes each time
        BUFFER_SIZE = 4096
        SEPARATOR = "<SEPARATOR>"

        # create the server socket
        # TCP socket
        s = socket.socket()

        s.bind((SERVER_HOST, SERVER_PORT))

        s.listen(50)

        # accept connection if there is any
        client_socket, address = s.accept() 
        # if below code is executed, that means the sender is connected

        # receive the file infos
        # receive using client socket, not server socket
        received = client_socket.recv(BUFFER_SIZE).decode()
        filename, filesize, destination = received.split(SEPARATOR)
        # remove absolute path if there is
        filename = os.path.basename(filename)
        # convert to integer
        filesize = int(filesize)

        # start receiving the file from the socket
        # and writing to the file stream
        successful_transfer = 0
        try:
            with open("temporary_file", "wb") as f:
                while(1):
                    # read 1024 bytes from the socket (receive)
                    bytes_read = client_socket.recv(BUFFER_SIZE)
                    if not bytes_read:    
                        # nothing is received
                        # file transmitting is done
                        break
                    # write to the file the bytes we just received
                    f.write(bytes_read)
                    # update the progress bar
                successful_transfer = 1

        finally:
            # close the server socket
            s.close()

            # close the client socket
            client_socket.close()

        if destination == "sdfs":
            if not os.path.isdir('./sdfs'):
                os.mkdir('./sdfs')

            os.rename('./' + "temporary_file", './sdfs/' + filename)
        
        elif destination == "local_fs":
            if successful_transfer:
                os.rename('./' + "temporary_file", filename)
                consts.requested_files_queue_lock.acquire()
                if address[0] in consts.REQUESTED_FILES_QUEUE:
                    consts.REQUESTED_FILES_QUEUE.pop(address[0])
                consts.requested_files_queue_lock.release()

        time.sleep(3)



def cliHandler():
    prompt = """Commands:
- put [localfilename] [sdfsfilename]
- get [sdfsfilename] [localfilename]
- delete [sdfsfilename]
- ls [sdfsfilename]
- store
"""

    print(prompt)

    while(True):
        user_input = input("- ")
        args = user_input.split(" ")
        
        opt = args[0]
        if opt == 'get' and len(args) == 3:
            send_get(args[1], args[2])
            print()

        elif opt == 'put' and len(args) == 3:
            send_put_request(args[1], args[2])
            print()

        elif opt == 'delete' and len(args) == 2:
            delete(args[1], membership_dict)
            print()

        elif opt == 'ls' and len(args) == 2:
            consts.membership_dict_lock.acquire()
            ls(args[1], membership_dict)
            consts.membership_dict_lock.release()
            print()

        elif opt == 'store' and len(args) == 1:
            store()
            print()

        else:
            print("Invalid input!")
            print(prompt)
            print()
        
#Invocation of all threads used in the program
sender_thread = threading.Thread(target = senderHandler, kwargs = {})
sender_thread.setDaemon(True)
sender_thread.start()

receiver_thread = threading.Thread(target = receiverHandler, kwargs = {})
receiver_thread.setDaemon(True)
receiver_thread.start()

protocol_thread = threading.Thread(target = protocolCheckerHandler, kwargs = {})
protocol_thread.setDaemon(True)
protocol_thread.start()

fail_check_thread = threading.Thread(target = failCheckHandler, kwargs = {})
fail_check_thread.setDaemon(True)
fail_check_thread.start()

leave_membership_thread = threading.Thread(target = leaveMembershipHandler, kwargs = {})
leave_membership_thread.setDaemon(True)
leave_membership_thread.start()

cli_thread = threading.Thread(target = cliHandler, kwargs = {})
cli_thread.setDaemon(True)
cli_thread.start()

file_transfer_thread = threading.Thread(target = fileTransferHandler, kwargs = {})
file_transfer_thread.setDaemon(True)
file_transfer_thread.start()


def introduceYourself():
    """
    Local function to introduce yourself to the Introducer

    :return: None
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    send_data = json.dumps({
                "type" : "Introduce",
                "msg_protocol_type": consts.PROTOCOL_TYPE
            })

    # send UDP packet to the Introducer
    sock.sendto(send_data.encode('utf-8'), (consts.INTRODUCER_IP, consts.DEFAULT_PORT))
    sock.close()

def waitForReply():
    """
    Local Fucntion to recieve an ACK from the Introducer

    :return: None
    """

    #Create a socket to listen from
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = (consts.SELF_IP, consts.DEFAULT_PORT)
    sock.bind(server_address)

    data, address = sock.recvfrom(consts.BUFFER_SIZE)
    # Only need the ip address
    address = address[0]

    assert(address == consts.INTRODUCER_IP) #need to receive from introducer
    data = json.loads(data)

    #data here should be 2D dictionary of membership list
    #update my own membership list
    for each_member in data:
        ID = each_member["id"]
        if (each_member["ip_addr"] == consts.SELF_IP):
            consts.SELF_UNIQUE_ID = ID
        temp = {
                    "ip_addr": each_member["ip_addr"],
                    "port_number": each_member["port_number"],
                    "heartbeat_count": each_member["heartbeat_count"],
                    "unix_time": each_member["unix_time"],
                    "msg_protocol_type": each_member["msg_protocol_type"]
                }
        membership_dict[ID] = temp

        #Log output to the logger file accordingly
        if each_member["ip_addr"] in address_host_dict:
            consts.logger.info("Added node at IP address: " + address_host_dict[each_member["ip_addr"]])
        else:
            consts.logger.info("Added node at IP address: " + each_member["ip_addr"])
    
    sock.close()

if consts.SELF_IP == consts.INTRODUCER_IP:

    # if we are the introducer, do nothing
    ID = generate_id(consts.INTRODUCER_IP)
    consts.SELF_UNIQUE_ID = ID
    membership_list = {
                        "ip_addr": consts.INTRODUCER_IP,
                        "port_number": consts.DEFAULT_PORT,
                        "heartbeat_count": 1,
                        "unix_time": time.time(),
                        "msg_protocol_type": consts.PROTOCOL_TYPE
                    }
    membership_dict[ID] = membership_list

    if consts.SELF_IP in address_host_dict:
        consts.logger.info("Added node at : " + address_host_dict[consts.SELF_IP])
    else:
        consts.logger.info("Added node at : " + consts.SELF_IP)
    # Introducer 
    consts.HAVE_BEEN_INTRODUCED = 1
else:
    # Register myself with introducer
    # 1. sendTO
    # 2. waitforaRecvFrom
    # 3. Update current membership list
    
    # Need to update membershiplist to have a column for just newcomer or not
    introduceYourself()
    waitForReply()
    consts.HAVE_BEEN_INTRODUCED = 1

# Wait for all threads to finish
main_thread = threading.current_thread()
for t in threading.enumerate():
    if t is main_thread:
        continue
    t.join()
