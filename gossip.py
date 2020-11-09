import threading
import socket
import time
import json
import random
import uuid
import os
import sys

from random import seed
from random import randint
from master import *
from client import *

import consts

from introducer import *
'''
Gossip Specific Membership Handling
'''

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


def getRandomListOfNodes(membership_dict):
    """
    Chooses a random subset of nodes from a node's membership list

    :return: List
    """

    random_ids = random.sample(list(membership_dict), k=min(consts.NUMBER_OF_NODES_TO_SEND_HEARBEAT, len(list(membership_dict))))
    random_nodes = {}
    for id in random_ids:
        random_nodes[id] = membership_dict[id]
    return random_nodes

 

def gossipSenderHandler(sock, self_id, membership_dict):
    """
    Packet sender handler for Gossip Heartbeating protocol

    Arguments:
    sock -- the socket used for sending packets
    membership_dict -- the node's membership list

    :return: None
    """
    consts.SELF_CURRENT_HEARTBEAT_COUNT += 1
    membership_dict[consts.SELF_UNIQUE_ID]["heartbeat_count"] += 1
    random_nodes = getRandomListOfNodes(membership_dict)

    consts.gossip_sender_queue_lock.acquire()
    while len(consts.GOSSIP_SENDER_QUEUE) > 0:
        (address, message) = consts.GOSSIP_SENDER_QUEUE.pop(0)
        sock.sendto(message.encode('utf-8'), (address, consts.DEFAULT_PORT))
    consts.gossip_sender_queue_lock.release()

    for host in random_nodes: 
        assert(self_id is not None)
        if membership_dict[host]["ip_addr"] != consts.SELF_IP:
            consts.num_packets_lock.acquire()
            consts.NUM_PACKETS += 1
            
            send_data = json.dumps(membership_dict)
            
            consts.ALL_TO_ALL_PACKET_SIZE = sys.getsizeof(send_data)
            
            sock.sendto(send_data.encode('utf-8'), (membership_dict[host]["ip_addr"], consts.DEFAULT_PORT))
            
            consts.num_packets_lock.release()

def gossipReceiverHandler(sock, membership_dict):
    """
    Packet receiver handler for Gossip Heartbeating protocol

    Arguments:
    sock -- the socket used for receiving packets
    membership_dict -- the node's membership list

    :return: None
    """

    data, address = sock.recvfrom(consts.BUFFER_SIZE)

    # Only need the ip address
    address = address[0]
    
    data = json.loads(data)
    # We should get the lock only after we have received some data

    consts.membership_dict_lock.acquire()

    if "type" in data and data["type"] == "PUT" and consts.SELF_IP == consts.MASTER_IP:
        reply_to_put(data["sdfs_file_name"], data["file_size"], data["local_file_name"], membership_dict, address, data["client_id"])
        consts.membership_dict_lock.release()
        return
    
    if "type" in data and data["type"] == "REPLY_FROM_PUT":
        receive_put_response(data["file_table_entry"], data["local_file_name"], membership_dict)
        consts.membership_dict_lock.release()
        return
    
    if "type" in data and data["type"] == "PUT_ACK" and consts.SELF_IP == consts.MASTER_IP:
        acknowledge_put_ack(data["sdfs_file_name"], membership_dict)
        consts.membership_dict_lock.release()
        return

    if "type" in data and data["type"] == "GET" and consts.SELF_IP == consts.MASTER_IP:
        reply_to_get(data["sdfs_file_name"], data["local_file_name"], membership_dict, address, data["client_id"])
        consts.membership_dict_lock.release()
        return

    if "type" in data and data["type"] == "REPLY_FROM_GET":
        receive_get(data["sdfs_file_name"], data["local_file_name"], data["file_size"], data["replica_nodes"], membership_dict)
        consts.membership_dict_lock.release()
        return

    if "type" in data and data["type"] == "ASK_FOR_FILE":
        fullfil_file_request(data, membership_dict)
        consts.membership_dict_lock.release()
        return    

    if "type" in data and data["type"] == "LS" and consts.SELF_IP == consts.MASTER_IP:
        reply_to_ls(data["sdfs_file_name"], address)
        consts.membership_dict_lock.release()
        return

    if "type" in data and data["type"] == "REPLY_FROM_LS":
        receive_ls(data["replica_nodes"], membership_dict)
        consts.membership_dict_lock.release()
        return

    if "type" in data and data["type"] == "DELETE" and consts.SELF_IP == consts.MASTER_IP:
        reply_to_delete(data["sdfs_file_name"], address, membership_dict)
        consts.membership_dict_lock.release()
        return

    if "type" in data and data["type"] == "REPLY_FROM_DELETE":
        receive_delete(data['sdfs_file_name'], address)
        consts.membership_dict_lock.release()
        return
        
    # check if it is the Intro or not
    if "type" in data and data["type"] == "Introduce" and consts.SELF_IP == consts.INTRODUCER_IP:
        introducer_main(membership_dict, address, "GOSSIP")
        consts.membership_dict_lock.release()
        return

    #voluntairly leave now
    if "leaving" in data:
        ID = data["id"]
        if ID in membership_dict:
            membership_dict.pop(ID)
        consts.membership_dict_lock.release()
        return

    # if inside gossip and receive all-to-all, then handle it all-to-all wise
    # since this might be all-to-all, need a for loop to iterate over the entore membership list 
    for individual_member in data:
        if type(data[individual_member]) is not dict and len(data) > 1:
            other_id = data["id"]

            if other_id in membership_dict:
                membership_dict[other_id]["unix_time"] = time.time()
                membership_dict[other_id]["heartbeat_count"] += 1
            else:
                membership_list = {
                            "ip_addr": address,
                            "port_number": consts.DEFAULT_PORT,
                            "heartbeat_count": 1,
                            "unix_time": time.time(),
                            "msg_protocol_type": "ALLTOALL"
                        }
                membership_dict[other_id] = membership_list
                # consts.logger.info("Added node at IP address: " + ip_address)
                if ip_address in address_host_dict:
                    consts.logger.info("a Added node at: " + address_host_dict[ip_address])
                else:
                    consts.logger.info("b Added node at: " + ip_address)
        else: 
            break
        consts.membership_dict_lock.release()
        return
    
    consts.contention_to_be_failed_lock.acquire()
    to_remove = set()
    
    for id in membership_dict:
        if address == membership_dict[id]["ip_addr"]:
            for pair in consts.GOSSIP_CONTENTION_TO_BE_FAILED_NODES:
                if pair[0] == id:
                    to_remove.add(pair)

    consts.GOSSIP_CONTENTION_TO_BE_FAILED_NODES = consts.GOSSIP_CONTENTION_TO_BE_FAILED_NODES - to_remove
    consts.contention_to_be_failed_lock.release()

    other_membership_dict = data

    for other_id in other_membership_dict:
        if other_id not in membership_dict:
            cur_time = time.time()
            if other_membership_dict[other_id]["ip_addr"] in consts.RECENTLY_REMOVED_NODES and (cur_time - consts.RECENTLY_REMOVED_NODES[other_membership_dict[other_id]["ip_addr"]]) < 2.5:
                continue

            if other_membership_dict[other_id]["ip_addr"] in address_host_dict:
                consts.logger.info("c Added node at: " + address_host_dict[other_membership_dict[other_id]["ip_addr"]])
            else:
                consts.logger.info("d Added node at: " + other_membership_dict[other_id]["ip_addr"])
            
            membership_dict[other_id] = other_membership_dict[other_id]
        else:
            if other_membership_dict[other_id]["heartbeat_count"] > membership_dict[other_id]["heartbeat_count"]:
                membership_dict[other_id]["heartbeat_count"] = other_membership_dict[other_id]["heartbeat_count"]
                membership_dict[other_id]["unix_time"] = time.time()
    
    consts.membership_dict_lock.release()
    

def gossipFailCheckHandler(membership_dict):
    """
    Failure checker handler for Gossip Heartbeating protocol

    Arguments:
    sock -- the socket used for receiving packets
    membership_dict -- the node's membership list

    :return: None
    """
   
    # remove a node from gossip
    
    remove_now = set()
    failed_move_to_remove = []

    consts.contention_to_be_failed_lock.acquire()
    current_time = time.time()
    for id in membership_dict:
        if id != consts.SELF_UNIQUE_ID:
            if (current_time - membership_dict[id]["unix_time"]) >= consts.TIME_INTERVAL_FAIL and (current_time - membership_dict[id]["unix_time"]) < (consts.TIME_INTERVAL_FAIL + consts.TIME_INTERVAL_CLEANUP):
                
                id_already_added_before_in_failed_nodes = False
                for pair in consts.GOSSIP_CONTENTION_TO_BE_FAILED_NODES:
                    if pair[0] == id:
                        id_already_added_before_in_failed_nodes = True
                        break
                
                if not id_already_added_before_in_failed_nodes:
                    if membership_dict[id]["ip_addr"] in address_host_dict:
                        consts.logger.info("Suspected failure for node at " + address_host_dict[membership_dict[id]["ip_addr"]])
                    else:
                        consts.logger.info("Suspected failure for node at " + membership_dict[id]["ip_addr"])

                
                consts.GOSSIP_CONTENTION_TO_BE_FAILED_NODES.add((id, current_time))
                # print("Suspected failure for node : ", membership_dict[id]["ip_addr"]) #needs to be logged
            elif (current_time - membership_dict[id]["unix_time"]) > (consts.TIME_INTERVAL_FAIL + consts.TIME_INTERVAL_CLEANUP):
                remove_now.add(id)

    for id, time_failed in consts.GOSSIP_CONTENTION_TO_BE_FAILED_NODES:
        if (current_time - time_failed) > consts.TIME_INTERVAL_CLEANUP:
            remove_now.add(id)
            failed_move_to_remove.append((id, time_failed))

    for id, time_failed in failed_move_to_remove:
        consts.GOSSIP_CONTENTION_TO_BE_FAILED_NODES.remove((id, time_failed))
    consts.contention_to_be_failed_lock.release()

    for id in remove_now:
        if id != consts.SELF_UNIQUE_ID:
            if id not in membership_dict:
                continue
            ip_address = membership_dict[id]["ip_addr"]

            if ip_address in address_host_dict:
                consts.logger.info("Removed node at " + address_host_dict[ip_address])
            else:
                consts.logger.info("Removed node at " + ip_address)
            
            consts.RECENTLY_REMOVED_NODES[ip_address] = time.time()
            
            handleNodeFailure(id, membership_dict)
            # print("Removed Id: ", ip_address) #needs to be logged
            membership_dict.pop(id)

