# Declraing consts file to resolve circular dependencies
import threading
import socket
import os 

from logger import *

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

HAVE_BEEN_INTRODUCED = 0 #Global variabel that just keeps track of whether we have been introduced or not

protocol_type_lock = threading.Lock() #Lock for our Protocol Type
membership_dict_lock = threading.Lock() #Lock for our Membership Dict Type

with open("protocol_type.txt", "r") as f:
    type_protocol = f.readline()
PROTOCOL_TYPE = type_protocol

# PROTOCOL_TYPE = os.environ.get('PROTOCOL_TYPE') # Gossip or All-To-All 
PROTOCOL_CHECKER_TIME = 5.0 #Make Protocol Handler Thread sleep for 5s

SELF_UNIQUE_ID = None # Assigned by introducer using generate_id helper function
SELF_IP = socket.gethostbyname(socket.gethostname())

NUMBER_OF_NODES_TO_SEND_HEARBEAT = 10 # Parameter for report analysis
DEFAULT_PORT = 4444 #Default PORT to listen to UDP packets
TIME_INTERVAL_GOSSIP = 0.5 #Time Interval for Send

INTRODUCER_IP = "172.22.156.55" # Host 10 (VM) static ID

TIME_INTERVAL_FAIL = 10 # Fail time is counted starting from time last heartbeat was received

TIME_INTERVAL_CLEANUP = 10 # Cleanup time is counted starting from time last heartbeat was received

MEMBERSHIP_FAIL_CHECK_TIME = 1 # Cleanup time is counted starting from time last heartbeat was received

BUFFER_SIZE = 4096 # Buffer Size for UDP payload

SELF_CURRENT_HEARTBEAT_COUNT = 0 # our VMs heartbeat count

contention_to_be_failed_lock = threading.Lock() # Gossip contention Lock
GOSSIP_CONTENTION_TO_BE_FAILED_NODES = set()

# logger = Logger("node_logs.log") # our Logger to dump constant logs to
logger = all_loggers('node_logger', 'node_logs.log')

#voluntarily leave the membership
LEAVE_MEMBERSHIP = os.environ.get('LEAVE_MEMBERSHIP')
LEAVE_MEMBERSHIP_TIME = 15

# Global variables used for BW and False positive calculation
NUM_PACKETS = 0 
num_packets_lock = threading.Lock()
ALL_TO_ALL_PACKET_SIZE = 0.0
PACKET_CHECK_TIME = 15
ONEKB = 1024

# packets_logger = Logger("membership_logs.log")
packets_logger = all_loggers('membership_logger', 'membership_logs.log')

# read_taking_place_by_id is either None or the nodes unique id from membership list
# id, name, size, replica_nodes, read_taking_place_by_node_id, write_taking_place_by_node_id, new_file_addition_by_node_id
file_table = {}
file_table_lock = threading.Lock() #file_table lock

error_logger = all_loggers('error_logger', 'error_logs.log')

NUM_REPLICAS = 3

MASTER_IP = INTRODUCER_IP

# Queue consisting of tuples like (address, json.dumps() data )
GOSSIP_SENDER_QUEUE = []
gossip_sender_queue_lock = threading.Lock() # Gossip Sender Queue Lock

REQUESTED_FILES_QUEUE = {} # Queue for all file that we have to access/operate on afterwards
requested_files_queue_lock = threading.Lock()

# in seconds
TIMEOUT_FOR_FILE_REQUEST = 10 

RECENTLY_REMOVED_NODES = {} # Keep a list of all nodes recently deleted

PENDING_MASTER_TASKS = [] # Taks that master has to perform when multiple concurrent execution happens
