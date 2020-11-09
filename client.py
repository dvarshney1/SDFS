import os
import consts
import socket
import time
import json
import shutil
import time
import random

def send_put_request(local_file_name, sdfs_file_name):
    """
    Handler For Sending the PUT Request to Master for storing a file in the SDFS

    local_file_name: The file name of the file to be sent stored in the local file system
    sdfs_file_name: The name by which we want to the store the file in SDFS

    :return: None
    """
    if os.path.isfile(local_file_name):
        local_file_size = os.path.getsize(local_file_name)
        
        send_data = json.dumps({
                "type" : "PUT",
                "local_file_name": local_file_name,
                "sdfs_file_name": sdfs_file_name,
                "file_size": local_file_size,
                "client_id": consts.SELF_UNIQUE_ID
        })
        
        consts.gossip_sender_queue_lock.acquire()
        consts.GOSSIP_SENDER_QUEUE.append((consts.MASTER_IP, send_data))
        consts.gossip_sender_queue_lock.release()

    else:
        print("Invalid file!")
        consts.error_logger.info('Wrong file path in send_put')


def receive_put_response(file_table_entry, local_file_name, membership_dict):
    """
    Handler For Receiving the PUT Response to Master for storing a file in the SDFS

    membership_dict: Current Node's Membership Dictionary
    local_file_name: The file name of the file to be sent stored in the local file system
    file_table_entry: The file_table of the master node sent to the client
    
    :return: None
    """
    sdfs_file_name = file_table_entry["name"]
    file_size = file_table_entry["size"]
    nodes_to_send_data_to = file_table_entry["replica_nodes"]
    
    for node in nodes_to_send_data_to:
        node_addr = membership_dict[node]["ip_addr"]
        send_file(node_addr, local_file_name, sdfs_file_name, "sdfs")

    #TODO: Send an ACK to Master than write happened so that we can update new_file_addition
    send_data = json.dumps({
                "type" : "PUT_ACK",
                "sdfs_file_name": sdfs_file_name
        })
    consts.gossip_sender_queue_lock.acquire()
    consts.GOSSIP_SENDER_QUEUE.append((consts.MASTER_IP, send_data))
    consts.gossip_sender_queue_lock.release()

def send_file(recepient_addr, local_file_name, sdfs_file_name, destination):
    """
    Handler For Sending File from client

    recepient_addr: address of the receipient to whom we need to send the file
    local_file_name: file name by which this file is stored locally in the file system
    sdfs_file_name: file name by which this file is stored in the SDFS 
    destination: indication to the reciever to store it in the local file system or SDFS
    
    :return: None
    """
    if consts.SELF_IP == recepient_addr:
        if not os.path.isdir('./sdfs'):
            os.mkdir('./sdfs')

        shutil.copy2(local_file_name, './sdfs/' + sdfs_file_name)
        return

    
    SEPARATOR = "<SEPARATOR>"
    BUFFER_SIZE = 4096 # send 4096 bytes each time step

    # the ip address or hostname of the server, the receiver
    host = recepient_addr
    
    # the port, let's use 5001
    port = 5001
    
    # the name of file we want to send, make sure it exists
    if destination == "sdfs":
        filename = local_file_name
    else:
        filename = './sdfs/' + local_file_name
    
    # get the file size
    filesize = os.path.getsize(filename)
    
    # create the client socket    
    s = socket.create_connection((host, port))

    # send the filename, filesize and destination
    s.send(f"{sdfs_file_name}{SEPARATOR}{filesize}{SEPARATOR}{destination}".encode())

    time.sleep(1)
    # start sending the file

    try:
        with open(filename, "rb") as f:
            while(1):
                # read the bytes from the file
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read:
                    # file transmitting is done
                    break
                # we use sendall to assure transimission in 
                # busy networks
                s.sendall(bytes_read)
            
    # close the socket
    finally:
        s.close()

def delete(sdfs_file_name, membership_dict):
    """
    Handler for deleting a file from the SDFS

    sdfs_file_name: file name by which this file is stored in the SDFS 
    membership_dict: the membership_dict of the current node
    
    :return: None
    """
    if consts.SELF_IP == consts.MASTER_IP:
        #if file is being written at wait
        while consts.file_table[sdfs_file_name]['write_taking_place_by_node_id'] is not None:
            time.sleep(1)

        #master called for delete
        consts.file_table[sdfs_file_name]['write_taking_place_by_node_id'] = consts.SELF_UNIQUE_ID
        list_of_replica_nodes = consts.file_table[sdfs_file_name]['replica_nodes']

        if sdfs_file_name in consts.file_table:
            consts.file_table.pop(sdfs_file_name)

        for node in list_of_replica_nodes:
            IP_address = membership_dict[node]["ip_addr"]
            if IP_address == consts.MASTER_IP:
                continue

            send_data = json.dumps({
                "type" : "REPLY_FROM_DELETE",
                "sdfs_file_name": sdfs_file_name,
            })

            consts.gossip_sender_queue_lock.acquire()
            consts.GOSSIP_SENDER_QUEUE.append((IP_address, send_data))
            consts.gossip_sender_queue_lock.release()

            current_dir = os.getcwd() + '/sdfs/'
            file_to_remove = current_dir + sdfs_file_name
            if os.path.isfile(file_to_remove):
                os.remove(file_to_remove)

    else:
        send_delete_request(sdfs_file_name)
        #need to inform the master to do the avoe step

def send_delete_request(sdfs_file_name):
    """
    Handler for sending the delete request to master

    sdfs_file_name: file name by which this file is stored in the SDFS 
    
    :return: None
    """
    send_data = json.dumps({
                "type" : "DELETE",
                "sdfs_file_name": sdfs_file_name,
                "client_id": consts.SELF_UNIQUE_ID
    })

    consts.gossip_sender_queue_lock.acquire()
    consts.GOSSIP_SENDER_QUEUE.append((consts.MASTER_IP, send_data))
    consts.gossip_sender_queue_lock.release()

def receive_delete(sdfs_file_name, client_address):
    """
    Handler for receiving delete from clients and delete the file

    sdfs_file_name: file name by which this file is stored in the SDFS 
    client_address: address of the client
    
    :return: None
    """
    #client has received this, delete file from current sdfs directory system
    if sdfs_file_name == None:
        #file already deleted (does not exist in master)
        print("File Does Not Exist!")
    else:
        current_dir = os.getcwd() + '/sdfs/'
        file_to_remove = current_dir + sdfs_file_name
        os.remove(file_to_remove)
        if sdfs_file_name in consts.file_table:
            consts.file_table.pop(sdfs_file_name)
        


def ls(sdfs_file_name, membership_dict):
    """
    Handler for receiving listing all nodes where a sdfs file is stored

    sdfs_file_name: file name by which this file is stored in the SDFS 
    membership_dict: membership_dict of the node
    
    :return: None
    """
    if consts.SELF_IP == consts.MASTER_IP:
        # ls called from the master only. Check for the file_name and print out all the nodes where it is stored
        if sdfs_file_name in consts.file_table:
            # print it out here
            list_of_receive_nodes = consts.file_table[sdfs_file_name]['replica_nodes']
            print("This file is contained at:")
            for node in list_of_receive_nodes:
                if node in membership_dict:
                    print(consts.address_host_dict[membership_dict[node]["ip_addr"]])
        else:
            print("The specified file does not exist in the SDFS") #go to next line, put in the wrong name
    else:
        #send the update to master
        send_ls_request(sdfs_file_name)

def send_ls_request(sdfs_file_name):
    """
    Handler for receiving request for listing all nodes where a sdfs file is stored

    sdfs_file_name: file name by which this file is stored in the SDFS 
    
    :return: None
    """
    send_data = json.dumps({
                "type" : "LS",
                "sdfs_file_name": sdfs_file_name,
                "client_id": consts.SELF_UNIQUE_ID
    })

    consts.gossip_sender_queue_lock.acquire()
    consts.GOSSIP_SENDER_QUEUE.append((consts.MASTER_IP, send_data))
    consts.gossip_sender_queue_lock.release()


def receive_ls(replica_nodes_list, membership_dict):
    """
    Handler for receiving all nodes where a sdfs file is stored

    replica_nodes_list: all nodes in SDFS where this file is stored 
    membership_dict: membership_dict of the node
    
    :return: None
    """
    if len(replica_nodes_list) == 0:
        #list is empty, print an empty line
        print("The specified file does not exist in the SDFS")
    for node in replica_nodes_list:
        if node in membership_dict:
            print(consts.address_host_dict[membership_dict[node]["ip_addr"]])

def store():
    """
    Handler for seeing all files stored in the current process
    
    :return: None
    """
    # have to loop through the sdfs file_system for this one
    current_dir = os.getcwd()
    current_dir += '/sdfs/'
    
    try:
        files_list = os.listdir(current_dir)
    except:
        print("SDFS Directory has not been created yet")
        return

    if len(files_list) == 0:
        print("No Files are replicated at this process -> SDFS directory is empty!")
        return

    for i in range(len(files_list)):
        print(files_list[i])

def send_get(sdfs_file_name, local_file_name):
    """
    Handler for sending a get request to master to retrieve a file from SDFS to local file system

    sdfs_file_name: file name to replicate from in SDFS
    local_file_name: file name to save it with in local file system
    
    :return: None
    """
    # Look in mp2/sdfs not mp2
    if os.path.isfile("./sdfs/" + sdfs_file_name):
        print("File already exists in local sdfs filesystem")
        return
    
    send_data = json.dumps({
                "type" : "GET",
                "local_file_name": local_file_name,
                "sdfs_file_name": sdfs_file_name,
                "client_id": consts.SELF_UNIQUE_ID
    })

    consts.gossip_sender_queue_lock.acquire()
    consts.GOSSIP_SENDER_QUEUE.append((consts.MASTER_IP, send_data))
    consts.gossip_sender_queue_lock.release()

def ask_random_replica_node_for_file(sdfs_file_name, local_file_name, file_size, replica_nodes, membership_dict):
    """
    Helper to retrieve a file from a random replica node

    sdfs_file_name: file name to replicate from in SDFS
    local_file_name: file name to save it with in local file system
    file_size: file size of the file in the sdfs system
    replica_nodes: list of nodes where this file is replicated in
    membership_dict: membership dictionary of the current node
    
    :return: None
    """
    random.shuffle(replica_nodes)
    random_node = replica_nodes[0]
    send_data = json.dumps({
                "type": "ASK_FOR_FILE",
                "local_file_name": local_file_name,
                "sdfs_file_name": sdfs_file_name,
                "client_id": consts.SELF_UNIQUE_ID,
                "requested_file_size": file_size
        })

    consts.gossip_sender_queue_lock.acquire()
    consts.GOSSIP_SENDER_QUEUE.append((membership_dict[random_node]["ip_addr"], send_data))
    consts.gossip_sender_queue_lock.release()
    
    consts.REQUESTED_FILES_QUEUE[membership_dict[random_node]["ip_addr"]] =  {"request_time" :time.time(), "replica_nodes": replica_nodes,
        "sdfs_file_name" : sdfs_file_name, "local_file_name" : local_file_name, "file_size" : file_size}
    


def receive_get(sdfs_file_name, local_file_name, file_size, replica_nodes, membership_dict):
    """
    Handler for receiving a GET request from the master

    sdfs_file_name: file name to replicate from in SDFS
    local_file_name: file name to save it with in local file system
    file_size: file size of the file in the sdfs system
    replica_nodes: list of nodes where this file is replicated in
    membership_dict: membership dictionary of the current node
    
    :return: None
    """
    if file_size == 0:
        print(sdfs_file_name + " does not exist!")
        return
        
    consts.requested_files_queue_lock.acquire()
    ask_random_replica_node_for_file(sdfs_file_name, local_file_name, file_size, replica_nodes, membership_dict)
    consts.requested_files_queue_lock.release()


def fullfil_file_request(data, membership_dict):
    """
    Handler for replicating file when/after a node crashes

    data: data to be replicated
    membership_dict: membership dictionary of the current node
    
    :return: None
    """
    node_addr = membership_dict[data["client_id"]]["ip_addr"]
    local_file_name = data["local_file_name"]
    sdfs_file_name = data["sdfs_file_name"]
    
    if "RE_REPLICATE" in data and data["RE_REPLICATE"] is True:
        send_file(node_addr, local_file_name, sdfs_file_name,  "sdfs")
    else:
        # CHANGED THE ORDER OF SDFS_FILE_NAME and LOCAL_FILE_NAME on purpose after going through code. DO NOT CHANGE!
        send_file(node_addr, sdfs_file_name, local_file_name, "local_fs")
