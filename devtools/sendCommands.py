import socket
import json
import threading
import time

'''
Run this file with python2

'''
'''
Execute any command in any server.
Make sure to execute pkill -9 python3 before launching another python process
'''

address_host_dict = {
    # "172.22.156.52": "fa20-cs425-g16-01",
    # "172.22.158.52": "fa20-cs425-g16-02",
    # "172.22.94.52": "fa20-cs425-g16-03",
    # "172.22.156.53": "fa20-cs425-g16-04",
    # "172.22.158.53": "fa20-cs425-g16-05",
    "172.22.94.53": "fa20-cs425-g16-06",
    "172.22.156.54":  "fa20-cs425-g16-07",
    "172.22.158.54": "fa20-cs425-g16-08",
    "172.22.94.54": "fa20-cs425-g16-09",
    "172.22.156.55": "fa20-cs425-g16-10"
}
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

def sendCommand():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    while(1):
        print("Type the command that needs to be executed")
        cmd = raw_input()
        if cmd == "ks":
            cmd = 'pkill -9 python3'
        elif cmd == "ss":
            cmd = 'python3 server.py'
        elif cmd == "gs":
            cmd = 'git stash'
        elif cmd == "gp":
            cmd = 'git pull'
            
        print("Server indices to send command to eg:03 04. Press . to send it to all servers")
        servers = raw_input().split()

        if len(servers) == 1 and servers[0] == ".":
            for address in address_host_dict:
                # time.sleep(5)
                send_data = json.dumps(cmd)
                sock.sendto(send_data.encode('utf-8'), (address, 6969))
        else:
            for server in servers:
                # time.sleep(5)
                send_data = json.dumps(cmd)
                sock.sendto(send_data.encode('utf-8'), (host_address_dict["fa20-cs425-g16-" + server], 6969))
                

    sock.close()

def receiveAcks():

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    SELF_IP = socket.gethostbyname(socket.gethostname())
    server_address = (SELF_IP, 6968)
    sock.bind(server_address)

    while(1):
        data, address = sock.recvfrom(6968)
        data = json.loads(data)
        print "Received ACK from vm " + str(data)


thread = threading.Thread(target = sendCommand, kwargs = {})
thread.setDaemon(True)
thread.start()

thread = threading.Thread(target = receiveAcks, kwargs = {})
thread.setDaemon(True)
thread.start()

main_thread = threading.current_thread()
for t in threading.enumerate():
    if t is main_thread:
        continue
    t.join()
