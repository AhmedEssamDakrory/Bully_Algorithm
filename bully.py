import zmq
import time
import parse
import sys
import threading

class Bully:
    def __init__(self, proc_ip, proc_port, proc_port2, id):
        conf_file = open("file.config","r")
        input = conf_file.readlines()
        n = int(input[0])
        self.processes = []
        self.maxId = 0 
        for i in range(1,n+1):
            line = parse.parse('{} {} {} {}', input[i])
            self.maxId = max(self.maxId, int(line[3]))
            self.processes.append({'ip': line[0], 'port': [line[1], line[2]], 'id': int(line[3])})
        self.proc_ip = proc_ip
        self.proc_port = proc_port
        self.id = id
        self.coor_id = -1
        self.proc_port2 = proc_port2

    def heart_beats(self, proc=None):
        if proc == 'coor':
            while int(self.coor_id) == int(self.id):
                self.heart_socket.send_string('alive {} {} {}'.format(self.proc_ip, self.proc_port, self.id))
                time.sleep(1)
        else:
            while True:
                try:
                    coor_heart_beat = self.heart_socket2.recv_string()
                    req = parse.parse('alive {} {} {}', coor_heart_beat)
                    if int(req[2]) > self.id:
                        print("coordinator  {}".format(coor_heart_beat))
                        self.update_coor(str(req[0]), str(req[1]), int(req[2]))
                except:
                    if self.coor_id != self.id:
                        print("Coordinator is dead, get ready for election \n")
                        self.coor_id = -1

    def establish_connection(self, TIMEOUT):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind('tcp://{}:{}'.format(self.proc_ip, self.proc_port2))
        self.socket2 = self.context.socket(zmq.REQ)
        self.socket2.setsockopt(zmq.RCVTIMEO, TIMEOUT)
        self.connect_to_higher_ids()
        self.heart_context = zmq.Context()
        self.heart_socket = self.heart_context.socket(zmq.PUB)
        self.heart_socket.bind('tcp://{}:{}'.format(self.proc_ip, self.proc_port))
        self.heart_socket2 = self.heart_context.socket(zmq.SUB)
        self.heart_socket2.setsockopt(zmq.RCVTIMEO, TIMEOUT)
        self.connect_all()
        self.heart_socket2.subscribe("")

    def connect_all(self):
        for p in self.processes:
            if int(p['id']) != int(self.id):
                self.heart_socket2.connect('tcp://{}:{}'.format(p['ip'], p['port'][0]))

    def connect_to_higher_ids(self):
        for p in self.processes:
            if int(p['id']) > int(self.id):
                self.socket2.connect('tcp://{}:{}'.format(p['ip'], p['port'][1]))
        # so that last process does not block on send...
        #self.socket2.connect('tcp://{}:{}'.format(p['ip'], 55555))

    def disconnect(self):
        for p in self.processes:
                self.socket2.disconnect('tcp://{}:{}'.format(p['ip'], p['port'][1]))

    def update_coor(self, ip, port, id):
        self.coor_ip = ip
        self.coor_port = port
        self.coor_id = id
    
    def declare_am_coordinator(self):
        print('I am the coordinator')
        self.update_coor(self.proc_ip, self.proc_port, self.id)
        heart_beats_thread = threading.Thread(target=self.heart_beats, args=['coor'])
        heart_beats_thread.start()

    def run_client(self):
        while True:
            if self.coor_id == -1:
                try:
                    if self.id == self.maxId:
                        self.declare_am_coordinator()
                    else:
                        self.socket2.send_string('election')
                        req = self.socket2.recv_string()
                except:
                    self.declare_am_coordinator()
                    
            time.sleep(1)

    def run_server(self):
        while True:
            request = self.socket.recv_string()
            if request.startswith('election'):
                #respond alive..
                self.socket.send_string('alive')

    def run(self):
        self.establish_connection(2000)

        heart_beats_thread = threading.Thread(target=self.heart_beats, args=[])
        heart_beats_thread.start()

        serv_thread = threading.Thread(target=self.run_server, args=[])
        serv_thread.start()

        client_thread = threading.Thread(target=self.run_client, args=[])
        client_thread.start()

ip, port1, port2, id = str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), int(sys.argv[4])

bully = Bully(ip, port1, port2, id)
bully.run()
