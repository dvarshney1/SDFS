import threading
import socket
import time
import json
import random
import uuid
import os

import consts

'''
Introducer File
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

# Membership List:
# Id	ip_addr		port number	 heartbeat_count	unix_time

def generate_id(address):
    """
    Generates a unique id for a given hostname

    Arguments:
    address -- the node's static IP address

    :return: string
    """
    unique_id = uuid.uuid4()
    return str(unique_id) + "-" + str(address)

def introducer_main(membership_dict, address, msg_protocol_type):
    """
    Introduces the host (address) and tells him the current protocol 

    Arguments:
    membership_dict -- the node's membership list
    address -- the node's static IP address
    msg_protocol_type -- tells whether it is ALLTOALL or GOSSIP

    :return: None
    """

    #get the uniqueID from the ipadress, update current membership list and create a socket
    ID = generate_id(address)
    membership_list = {
                        "ip_addr": address,
                        "port_number": consts.DEFAULT_PORT,
                        "heartbeat_count": 1,
                        "unix_time": time.time(),
                        "msg_protocol_type": msg_protocol_type
                    }
    membership_dict[ID] = membership_list
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    #send current membership list 
    data = []
    for ID in membership_dict:
        temp = {"id": ID}
        temp["ip_addr"] = membership_dict[ID]["ip_addr"]
        temp["port_number"] = consts.DEFAULT_PORT
        temp["heartbeat_count"] = membership_dict[ID]["heartbeat_count"]
        temp["unix_time"] = membership_dict[ID]["unix_time"]
        temp["msg_protocol_type"] = membership_dict[ID]["msg_protocol_type"]

        data.append(temp)
    
    send_data = json.dumps(data)

    #log to our output file that we have added a new node
    if address in address_host_dict:
        consts.logger.info("Added node at : " + address_host_dict[address])
    else:
        consts.logger.info("Added node at : " + address)
    sock.sendto(send_data.encode('utf-8'), (address, consts.DEFAULT_PORT))
    sock.close()

