import socket
import json
import subprocess

'''
Run this file with python2

'''
'''
Execute any command in any server.
Make sure to execute pkill -9 python before launching another python process
'''

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

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
SELF_IP = socket.gethostbyname(socket.gethostname())
server_address = (SELF_IP, 6969)
sock.bind(server_address)

send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
while(1):
    data, address = sock.recvfrom(6969)
    try:
        data = json.loads(data)
        print "Received command " + str(data)
        p = subprocess.Popen(data.split(), cwd="/home/pavitra3/cs425-mp1/mp2/")

        # Send ack
        address = address[0]
        ack = json.dumps(address_host_dict[SELF_IP][-2:])
        send_sock.sendto(ack.encode('utf-8'), (address, 6968))
    except:
        pass


send_sock.close()
sock.close()
