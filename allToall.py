import threading
import socket
import time
import json
import random
import uuid
import os
from introducer import *
import sys

from random import randint

import consts
'''
All to All specific Membership Handling
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


def allToAllReceiverHandler(sock, membership_dict):
    """
    Packet receiver handler for All-to-All Heartbeating protocol

    Arguments:
    sock -- the socket used for receiving packets
    membership_dict -- the node's membership list

    :return: None
    """

    data, address = sock.recvfrom(consts.BUFFER_SIZE)
    
    # We should get the lock only after we have received some data
    consts.membership_dict_lock.acquire()
    # print("membership_dict_lock Lock aquired")

    # Only need the ip address
    address = address[0]
    # print("Received from address ", address," : ", data.decode('utf-8'), "\n")
    
    data = json.loads(data)
    #check if it is the Intro or not
    if "type" in data and data["type"] == "Introduce" and consts.SELF_IP == consts.INTRODUCER_IP:
        introducer_main(membership_dict, address, "ALLTOALL")
        consts.membership_dict_lock.release()
        return
    
    #voluntairly leave now
    if "leaving" in data:
        ID = data["id"]
        if ID in membership_dict:
            membership_dict.pop(ID)
        consts.membership_dict_lock.release()
        return

    #if inside all to all and receive gossip, then handle it gossip wise
    # since this might be gossip, need a for loop to iterate over the entore membership list 
    for individual_member in data:
        if type(data[individual_member]) is dict and data[individual_member]["ip_addr"] == address and data[individual_member]["msg_protocol_type"] == 'GOSSIP':
            #handle it gossip wise (gossip recv)
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
                    if other_membership_dict[other_id]["ip_addr"] in address_host_dict:
                        consts.logger.info("Added node at: " + address_host_dict[other_membership_dict[other_id]["ip_addr"]])
                    else:
                        consts.logger.info("Added node at: " + other_membership_dict[other_id]["ip_addr"])
                    membership_dict[other_id] = other_membership_dict[other_id]
                else:
                    if other_membership_dict[other_id]["heartbeat_count"] > membership_dict[other_id]["heartbeat_count"]:
                        membership_dict[other_id]["heartbeat_count"] = other_membership_dict[other_id]["heartbeat_count"]
                        membership_dict[other_id]["unix_time"] = time.time()
            consts.membership_dict_lock.release()
            return

    # else if we receieve an All-to-All membership signal which is not Introduce or Voluntarily Leaving
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
        if address in address_host_dict:
            consts.logger.info("Added node at: " + address_host_dict[address])
        else:
            consts.logger.info("Added node at: " + address)

    # print("Received heartbeat from ", other_id[38:], membership_dict[other_id]["heartbeat_count"])
    consts.membership_dict_lock.release()
    
def allToAllSenderHandler(sock, self_id, membership_dict):
    """
    Packet sender handler for All-to-All Heartbeating protocol

    Arguments:
    sock -- the socket used for sending packets
    membership_dict -- the node's membership list

    :return: None
    """
    
    for id in membership_dict:
        assert(self_id is not None)
        if id != self_id:
            consts.num_packets_lock.acquire()
            consts.NUM_PACKETS += 1

            #create the data (json) that we wish to send
            send_data = json.dumps({
                "id" : self_id,
                "msg_protocol_type": "ALLTOALL"
            })
            # print("Sent heartbeat to ", host)
            
            consts.ALL_TO_ALL_PACKET_SIZE = sys.getsizeof(send_data)
            consts.num_packets_lock.release()
            
            sock.sendto(send_data.encode('utf-8'), (membership_dict[id]["ip_addr"], consts.DEFAULT_PORT))


def allToAllFailCheckHandler(membership_dict):
    """
    Fuilure checker handler handler for All-to-All Heartbeating protocol

    Arguments:
    membership_dict -- the node's membership list

    :return: None
    """
    current_time = time.time()
    failed_id = []
    for id in membership_dict:
        if id != consts.SELF_UNIQUE_ID:
            # print(id, consts.SELF_UNIQUE_ID)
            if (current_time - membership_dict[id]["unix_time"]) > consts.TIME_INTERVAL_CLEANUP:
                failed_id.append(id)
    
    #log to a file -> All Fail Id
    for id in failed_id:
        if id != consts.SELF_UNIQUE_ID:
            ip_address = membership_dict[id]["ip_addr"]
            if ip_address in address_host_dict:
                consts.logger.info("Removed node at IP address " + address_host_dict[ip_address])
            else:
                consts.logger.info("Removed node at IP address " + ip_address)
            membership_dict.pop(id)

