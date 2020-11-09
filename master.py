import uuid
import consts
import random
import json
import os
import time

"""
File which will contain utility functions that need to be run on master
"""

def reply_to_put(sdfs_file_name, file_size, local_file_name, membership_dict, client_addr, client_id):
    """
    Handler for replying to a PUT request by a client

    sdfs_file_name: file name to replicate from in SDFS
    local_file_name: file name to save it with in local file system
    file_size: file size of the file in the sdfs system
    client_addr: IP address of the client
    client_id: unique id of the client
    membership_dict: membership dictionary of the current node
    
    :return: None
    """
    
    if sdfs_file_name in consts.file_table:
        print("The specified file already exists in the SDFS")
        # consts.error_logger.info(sdfs_file_name, " already exists in file table")
        # Remove the file if it already exists and create a new id for the new contents of the file
        # If some old nodes already have the file and they are not present in the new replica nodes, they 
        # should just delete file automatically after seeing the new file table. If the old nodes are chosen for
        # as new replica nodes again, they dont need to make a change.
        consts.file_table.pop(sdfs_file_name)
        return
    
    assert(client_id is not None)

    unique_id = str(sdfs_file_name)
    
    master_node_id = "" 
    for id in membership_dict.keys():
        if membership_dict[id]["ip_addr"] == consts.MASTER_IP:
            master_node_id = id
    
    candidate_nodes = set(membership_dict.keys())
    candidate_nodes.remove(master_node_id)
    if master_node_id != client_id:
        candidate_nodes.remove(client_id)

    candidate_nodes = list(candidate_nodes)
    random.shuffle(candidate_nodes)
    replica_nodes = candidate_nodes[:min(len(candidate_nodes), consts.NUM_REPLICAS)]
    replica_nodes.append(master_node_id)
    if master_node_id != client_id:
        replica_nodes.append(client_id)

    consts.file_table[sdfs_file_name] = {   
                                            "name": sdfs_file_name,
                                            "size": file_size, 
                                            "replica_nodes": replica_nodes, 
                                            "read_taking_place_by_node_id": [],
                                            "write_taking_place_by_node_id": None,
                                            "new_file_addition_by_node_id": client_id
                                        }

    send_data = json.dumps({
                "type" : "REPLY_FROM_PUT",
                "file_table_entry": consts.file_table[sdfs_file_name],
                "local_file_name" : local_file_name
    })

    consts.gossip_sender_queue_lock.acquire()
    consts.GOSSIP_SENDER_QUEUE.append((client_addr, send_data))
    consts.gossip_sender_queue_lock.release()


def acknowledge_put_ack(sdfs_file_name, membership_dict):
    """
    Handler for acknowledging a PUT request from the master

    sdfs_file_name: file name to replicate from in SDFS
    membership_dict: membership dictionary of the current node
    
    :return: None
    """
    if sdfs_file_name in consts.file_table:
        consts.file_table[sdfs_file_name]['new_file_addition_by_node_id'] = None

    print(sdfs_file_name, " finished uploading successfully")

    # we can run all the pending tasks
    while len(consts.PENDING_MASTER_TASKS) > 0:
        task = consts.PENDING_MASTER_TASKS.pop(0)
        if task["type"] == "REPLY_TO_GET":
            reply_to_get(task["sdfs_file_name"], task["local_file_name"], membership_dict, task["client_addr"], task["client_id"])
        elif task["type"] == "REPLY_FROM_DELETE":
            reply_to_delete(task["sdfs_file_name"], task["client_addr"], membership_dict)

def reply_to_get(sdfs_file_name, local_file_name, membership_dict, client_addr, client_id):
    """
    Handler for replying to a GET request by a client

    sdfs_file_name: file name to replicate from in SDFS
    local_file_name: file name to save it with in local file system
    client_addr: IP address of the client
    client_id: unique id of the client
    membership_dict: membership dictionary of the current node
    
    :return: None
    """
    if sdfs_file_name in consts.file_table:
        if consts.file_table[sdfs_file_name]['new_file_addition_by_node_id'] is not None:
            consts.PENDING_MASTER_TASKS.append({"type": "REPLY_TO_GET", 
                                                "sdfs_file_name" : sdfs_file_name,
                                                "local_file_name" : local_file_name,
                                                "client_addr": client_addr, 
                                                "client_id": client_id})
            return

        consts.file_table[sdfs_file_name]["read_taking_place_by_node_id"].append(client_id)

        send_data = json.dumps({
                    "type" : "REPLY_FROM_GET",
                    "file_size": consts.file_table[sdfs_file_name]['size'],
                    "local_file_name": local_file_name,
                    "sdfs_file_name": sdfs_file_name,
                    "replica_nodes": consts.file_table[sdfs_file_name]['replica_nodes']
        })

        consts.gossip_sender_queue_lock.acquire()
        consts.GOSSIP_SENDER_QUEUE.append((client_addr, send_data))
        consts.gossip_sender_queue_lock.release()
        
    else:
        send_data = json.dumps({
                    "type" : "REPLY_FROM_GET",
                    "file_size": 0,
                    "local_file_name": local_file_name,
                    "sdfs_file_name": sdfs_file_name,
                    "replica_nodes": []
        })
        consts.gossip_sender_queue_lock.acquire()
        consts.GOSSIP_SENDER_QUEUE.append((client_addr, send_data))
        consts.gossip_sender_queue_lock.release()
        
def reply_to_ls(sdfs_file_name, client_address):
    """
    Handler for replying to a LS request by a client

    sdfs_file_name: file name to replicate from in SDFS
    client_address: IP address of the client
    
    :return: None
    """
    if sdfs_file_name in consts.file_table.keys():
        send_to_client_list_of_nodes = consts.file_table[sdfs_file_name]['replica_nodes']
        send_data = json.dumps({
                    "type" : "REPLY_FROM_LS",
                    "replica_nodes": consts.file_table[sdfs_file_name]['replica_nodes']
        })

        consts.gossip_sender_queue_lock.acquire()
        consts.GOSSIP_SENDER_QUEUE.append((client_address, send_data))
        consts.gossip_sender_queue_lock.release()
    else:
        send_data = json.dumps({
                    "type" : "REPLY_FROM_LS",
                    "replica_nodes": list()
        })

        consts.gossip_sender_queue_lock.acquire()
        consts.GOSSIP_SENDER_QUEUE.append((client_address, send_data))
        consts.gossip_sender_queue_lock.release()


def reply_to_delete(sdfs_file_name, client_address, membership_dict):
    """
    Handler for replying to a DELETE request by a client

    sdfs_file_name: file name to replicate from in SDFS
    client_address: IP address of the client
    membership_dict: membership dictionary of the current node
    
    :return: None
    """

    if sdfs_file_name in consts.file_table.keys():
        #if file is being written at wait
        if consts.file_table[sdfs_file_name]['new_file_addition_by_node_id'] is not None:
            consts.PENDING_MASTER_TASKS.append({"type": "REPLY_FROM_DELETE", 
                                                "sdfs_file_name" : sdfs_file_name, 
                                                "local_file_name" : None,
                                                "client_addr": client_address, 
                                                "client_id": None})
            return      
        
        #set_file_written to TRUE and start delete
        consts.file_table[sdfs_file_name]['write_taking_place_by_node_id'] = consts.SELF_UNIQUE_ID
        
        #send all replica nodes message to delete this file
        for node in consts.file_table[sdfs_file_name]['replica_nodes']:
            if node == consts.SELF_UNIQUE_ID:
                #master hs to delete the files
                current_dir = os.getcwd() + '/sdfs/'
                file_to_remove = current_dir + sdfs_file_name
                if os.path.isfile(file_to_remove):
                    os.remove(file_to_remove)
            else:
                send_data = json.dumps({
                    "type" : "REPLY_FROM_DELETE",
                    "sdfs_file_name" : sdfs_file_name
                })
                
                IP_address = membership_dict[node]["ip_addr"] 
                consts.gossip_sender_queue_lock.acquire()
                consts.GOSSIP_SENDER_QUEUE.append((IP_address, send_data))
                consts.gossip_sender_queue_lock.release()
        
    else:
        #cannot delete, #send data to client that we cannot delete instead of ACK
        send_data = json.dumps({
                    "type" : "REPLY_FROM_DELETE",
                    "sdfs_file_name": None
        })

        consts.gossip_sender_queue_lock.acquire()
        consts.GOSSIP_SENDER_QUEUE.append((client_address, send_data))
        consts.gossip_sender_queue_lock.release()
    
    #remove the entry from the file_table as well and send ACK to client (if you want)
    if sdfs_file_name in consts.file_table.keys():
        consts.file_table.pop(sdfs_file_name)

def handleNodeFailure(id, membership_dict): #id -> UNIQUE_ID
    """
    Handler for replying to a Node Failure operation by a client

    id: unique id of the client
    membership_dict: membership dictionary of the current node
    
    :return: None
    """
    for sdfs_file_name in consts.file_table:
        for node in consts.file_table[sdfs_file_name]["replica_nodes"]:
            if node == id:
                consts.file_table[sdfs_file_name]["replica_nodes"].remove(id)
                
                # we have alock on membeship_dict
                candidate_nodes = set(membership_dict.keys())
                
                for node in consts.file_table[sdfs_file_name]["replica_nodes"]:
                    candidate_nodes.remove(node)
                candidate_nodes.remove(id)
                if consts.SELF_UNIQUE_ID in candidate_nodes:
                    candidate_nodes.remove(consts.SELF_UNIQUE_ID)

                candidate_nodes = list(candidate_nodes)
                random.shuffle(candidate_nodes)
                if(len(candidate_nodes)) == 0:
                    consts.error_logger.info("No healthy node left to pick to store a file from a failed node")
                    continue
                random_node = candidate_nodes[0]
                
                send_data = json.dumps({
                    "type": "ASK_FOR_FILE",
                    "local_file_name": './sdfs/' + sdfs_file_name,
                    "sdfs_file_name": sdfs_file_name,
                    "client_id": random_node,
                    "requested_file_size": consts.file_table[sdfs_file_name]["size"],
                    "RE_REPLICATE" : True
                })

                #append this random node to file_table -> replica_nodes
                consts.file_table[sdfs_file_name]["replica_nodes"].append(random_node)
    
                randi_node = consts.file_table[sdfs_file_name]["replica_nodes"][0]
                IP_address = membership_dict[randi_node]['ip_addr']
                
                consts.gossip_sender_queue_lock.acquire()
                consts.GOSSIP_SENDER_QUEUE.append((IP_address, send_data))
                consts.GOSSIP_SENDER_QUEUE.append((consts.MASTER_IP, send_data))
                consts.gossip_sender_queue_lock.release()

