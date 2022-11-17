import threading
from socket import *
import os
import json
import time

class serverthread(threading.Thread):
    def __init__(self, nodeaddress, nodesocket, thread_id,lock):
        threading.Thread.__init__(self)
        self.nodeaddress = nodeaddress
        self.nodesocket = nodesocket
        self.thread_id = thread_id
        self.lock= lock

    def set_key_value(self,key,value):

    # Check if json exists or not1:
    # If not exists create a new json file and add a key value to the json 
        if os.path.isfile('key_value.json'.format(self.thread_id)):
            with self.lock:
                with open('key_value.json'.format(self.thread_id),'r') as json_file:
                    json_decoded = json.load(json_file)
        else:
            json_decoded={}
        # value=[int(x) for x in value[1:-1].split(",")]
        # if key in json_decoded:
        #     json_decoded[key]+=value
        json_decoded[key] = value
        
        with self.lock:
            with open('key_value.json'.format(self.thread_id), 'w') as json_file:
                json.dump(json_decoded, json_file)

    def get_key_value(self,key):

        temp=[]
        key="mapper {} ".format(key)
        print(key)
        with self.lock:
            with open('key_value.json') as json_file:
                data = json.load(json_file)
                for each in data:
                    if key in each:
                        temp.append(data[each])

        print(temp)
        return temp
    
    def generatewcoutput(self):
        print("Generating OUtput")

        with open('key_value.json'.format(self.thread_id),'r') as json_file:
                json_decoded = json.load(json_file)

        word_count_result= {}

        for each in json_decoded:
            if "reducer" in each:
                key= each.split(" ")[1]
                word_count_result[key]=json_decoded[each]
        
        with open('word_count.json', 'w') as json_file:
                json.dump(word_count_result, json_file)
    
    def generateinvindoutput(self):
        print("Generating OUtput")

        with open('key_value.json'.format(self.thread_id),'r') as json_file:
                json_decoded = json.load(json_file)

        word_count_result= {}

        for each in json_decoded:
            if "reducer" in each:
                key= each.split(" ")[1]
                word_count_result[key]=json_decoded[each]
        
        with open('inv_ind.json', 'w') as json_file:
                json.dump(word_count_result, json_file)


    def run(self):
        print("--"*50)
        print(self.nodeaddress,"Node Adress from Server Thread #{}".format(self.thread_id))
    
        while True:
            recv_data = self.nodesocket.recv(1024)  # Should be ready to read
            message = recv_data.decode('utf-8')
            split_message=message.split(" ")
            method= split_message[0].lower()
            if len(split_message)>1:
                print(split_message)

            if method=='set':
                key= split_message[1]+" "+split_message[2]+" "+str(self.thread_id)
                self.set_key_value(key,(" ").join(split_message[3:]))
                self.nodesocket.send("STORED".encode('utf-8'))

            elif method=='get':
                print("GET")
                key_ =split_message[1]
                print(key_)
                values=self.get_key_value(key_)
                # print("Values", values)
                print(len(values))
                print("------")
                self.nodesocket.send(str(len(values)).encode())
                for each_value in values:
                    while True:
                        recv_msg=self.nodesocket.recv(50)
                        if recv_msg.decode()=="sendnextvalue":
                            break
                    send_string= key_ +" "+ each_value
                    self.nodesocket.send(send_string.encode())

            elif method=="generatewcoutput":
                self.generatewcoutput()

            elif method=="generateinvindoutput":
                self.generateinvindoutput()
                                 
            elif method=="close":
                break

def start_key_value_node():
    print("Key Value Serve Started ")
    host,port='127.0.0.1', 10000
    keyvaluesocket = socket(AF_INET, SOCK_STREAM)
    keyvaluesocket.bind((host,port))
    thread_id=0
    lock = threading.Lock()
    while True:
        keyvaluesocket.listen()
        nodesocket, nodeaddress = keyvaluesocket.accept()
        newserverthread = serverthread(nodeaddress, nodesocket,thread_id,lock)
        newserverthread.start()
        print("Reached Here")
        thread_id+=1
        

if __name__=='__main__':
    start_key_value_node()