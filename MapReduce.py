
# Nodes and tasks are implemented as conventional OS processes.
import concurrent.futures
import threading
from socket import *
import time
import os
import re
import json
from datetime import datetime

class MapReduce(object):

    
    # Constructor for Map Reduce
    def __init__(self, config_file):

        with open(config_file) as json_file:
            data = json.load(json_file)

        self.function=data["function"]
        self.filelocations= data['inputlocations']
        self.no_of_mappers= data['no_of_mappers']
        self.no_of_reducers= data['no_of_reducers']
        self.master_adrress= data['master_adrress']
        self.master_port= data['master_port']
        self.kv_address= data['kv_address']
        self.kv_port= data['kv_port']

########################################## MAPPER FUNCTIONS ############################################
    # Helper function to clean data
    def clean_data(self,input_data):
        # I leveraged this regular expression code, not my own
        input_data= re.sub('[^0-9a-zA-Z]+',' ', input_data)
        input_data= input_data.split()
        return input_data
    
    # Helper funtion for mapping
    def map_receive_word_count(self, mappersocket, function,filename=None):
        data_for_mapping= mappersocket.recv(1048576)
        data_for_mapping= data_for_mapping.decode('utf-8')
        data_for_mapping= self.clean_data(data_for_mapping)

        mapped_value={}
        
        if function=="wordcount":
            for each in data_for_mapping:
                if len(each)>=1:
                    if each not in mapped_value:
                        mapped_value[each]=[1]
                    else:
                        mapped_value[each].append(1)

        elif function=="invertedindex":
            for each in data_for_mapping:
                if len(each)>=1:
                    if each not in mapped_value:
                        mapped_value[each]=[filename[0:len(filename)-4]]

        # print(mapped_value)
        # Creating Mapper's Socket and establishing a connection to one of the thread of master
        maptokvsocket = socket(AF_INET, SOCK_STREAM) 
        maptokvsocket.connect((self.kv_address,self.kv_port))

        for each in mapped_value:
            message= "set mapper "+each+" {0}".format(mapped_value[each])
            maptokvsocket.send(message.encode())
            while True:
                recv_msg=maptokvsocket.recv(1024)
                if recv_msg.decode()=="STORED":
                    break

        maptokvsocket.send("close".encode())
        maptokvsocket.close()
        mappersocket.send("Mapping Completed {}".format(id).encode('utf-8'))
        #print("----------------------------")
        # print(list(mapped_value.keys()))
        keys_sent= " ".join([x for x in list(mapped_value.keys()) if len(x)>=1])
        mappersocket.send(keys_sent.encode())
        # mappersocket.close()


    def mapper_count_word(self,id):
        print(datetime.now(),"| "+"Mapper {} started ".format(id))
        # Creating Mapper's Socket and establishing a connection to one of the thread of master
        mappersocket = socket(AF_INET, SOCK_STREAM) 
        mappersocket.connect((self.master_adrress,self.master_port))

        # Send acknowledge to master saying process started 
        mappersocket.send("Mapper {} ".format(id).encode('utf-8'))

        #Get the Message from Master. This will tell us which functionality master want mapper to proceed with
        while True:
            master_instruction=mappersocket.recv(256)
            if len(master_instruction)>1:
                break
        master_instruction=master_instruction.decode('utf-8')

        # print("@@@--@@@"*50, master_instruction)
        # print(master_instruction)
        if master_instruction=="wordcount":
            self.map_receive_word_count(mappersocket,master_instruction)

        elif master_instruction=="invertedindex":
            mappersocket.send("filename".encode())
            filename=mappersocket.recv(256)
            filename=filename.decode()
            mappersocket.send("DataPlease".encode())
            self.map_receive_word_count(mappersocket,master_instruction,filename)
            
        # mappersocket.close()
        print(datetime.now(),"| "+"Mapping Completed {}".format(id))
        return "Mapping Completed {}".format(id)

    def activate_map_process(self):
        time.sleep(1)
        print(datetime.now(),"| "+"Mapping Process Started")
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = [executor.submit(self.mapper_count_word, id ) for id in range(self.no_of_mappers)]
            for f in concurrent.futures.as_completed(results):
                print(f.result())

###########################################  REDUCER FUNCTION ##################################
    def reduce_receive_word_count(self, reducersocket, id,function):
        keys_for_reducing= reducersocket.recv(1048576)
        keys_for_reducing= keys_for_reducing.decode('utf-8')
        keys_for_reducing= keys_for_reducing.split(" ")
        # print(":-:"*100)
        # print(keys_for_reducing)

        reducetokvsocket = socket(AF_INET, SOCK_STREAM) 
        reducetokvsocket.connect((self.kv_address,self.kv_port))
        
        reduced_values={}

        # print(function, "REDUCED","------------------")

        for each in keys_for_reducing:
            if function=="wordcount":
                if each not in reduced_values:
                    reduced_values[each]=0
            elif function=="invertedindex":
                if each not in reduced_values:
                    reduced_values[each]=[]
            # time.sleep(0.1)
            value="get "+ each
            reducetokvsocket.send(value.encode())
            num_keys=reducetokvsocket.recv(50)

            num_keys= int(num_keys.decode())

            # print("-_-"*30, num_keys)
            for _ in range(num_keys):
                reducetokvsocket.send("sendnextvalue".encode())
                while True:
                    data=reducetokvsocket.recv(100000)
                    data=data.decode()
                    if len(data)>1:
                        # print(data)
                        data=data.split(" ")
                        key_= data[0]
                        values_= "".join(data[1:])[1:-1].split(",")
                        if function=="wordcount":
                            values_ = sum([int(x) for x in values_])
                            reduced_values[key_]+=values_
                        elif function=="invertedindex":
                            reduced_values[key_]+=values_

                        break

        # print(reduced_values)
        
        for each in reduced_values:
            message= "set reducer "+each+" {}".format(reduced_values[each])
            reducetokvsocket.send(message.encode())
            while True:
                recv_msg=reducetokvsocket.recv(1024)
                # print(recv_msg)
                if recv_msg.decode()=="STORED":
                    break
            # print(recv_msg.decode())
        reducetokvsocket.send("close".encode())
        # reducetokvsocket.close()
        print(datetime.now(),"| "+"Reducing Completed {}".format(id))
        reducersocket.send("Reducing Completed {}".format(id).encode('utf-8'))
        # reducersocket.close()

        
    def reduce_count_word(self,id):
        print(datetime.now(),"| "+"Reducer {} started ".format(id))
        # Creating Mapper's Socket and establishing a connection to one of the thread of master
        reducersocket = socket(AF_INET, SOCK_STREAM) 
        reducersocket.connect((self.master_adrress,self.master_port))

        # Send acknowledge to master saying process started 
        reducersocket.send("Reducer {} ".format(id).encode('utf-8'))

        #Get the Message from Master. This will tell us which functionality master want mapper to proceed with
        master_instruction=reducersocket.recv(256)
        master_instruction=master_instruction.decode('utf-8')
        reducersocket.send("Keysplease".encode())

        self.reduce_receive_word_count(reducersocket, id,master_instruction)

        

        # reducersocket.close()

    def activate_reduce_process(self):
        print(datetime.now(),"| "+"Reduction Process Started")
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = [executor.submit(self.reduce_count_word, id) for id in range(self.no_of_reducers)]
            # for f in concurrent.futures.as_completed(results):
            #     print(f.result())


    class masterthread(threading.Thread):
        def __init__(self, nodeaddress, nodesocket,thread_id,data,no_of_mappers, no_of_reducers, function, filename= None):
            threading.Thread.__init__(self)
            self.nodeaddress = nodeaddress
            self.nodesocket = nodesocket
            self.thread_id = thread_id
            self.no_of_mappers = no_of_mappers
            self.no_of_reducers = no_of_reducers
            self.data=data
            self.function=function
            self.filename=filename

        def run(self):
            global maps_completed
            global reds_completed
            # print("--"*50)
            print(datetime.now(),"| ",self.nodeaddress,"Node Adress from Master Thread #{}".format(self.thread_id))
            # self.nodesocket.settimeout(5.0)
            data=self.nodesocket.recv(256)

            if "Mapper" in data.decode('utf-8'):
                self.nodesocket.send(self.function.encode('utf-8'))
                if self.function=="invertedindex":
                    while True:
                        # print("dafjh")
                        data=self.nodesocket.recv(256)
                        data= data.decode()
                        if data=="filename":
                            self.nodesocket.send(self.filename.encode())
                            break
                    
                    while True:
                        data=self.nodesocket.recv(256)
                        data= data.decode()
                        print(data,"Master receipt")
                        if data=="DataPlease":
                            self.nodesocket.send(self.filename.encode())
                            break
                    
                # time.sleep(0.1)
                self.nodesocket.send(self.data.encode('utf-8'))
                # time.sleep(0.11)
                data=self.nodesocket.recv(256)

            elif "Reducer" in data.decode('utf-8'):

                # print(data.decode(),self.function, "Reducer Starting Confirmation")
                self.nodesocket.send(self.function.encode('utf-8'))
                data= self.nodesocket.recv(256)
                
                # time.sleep(1)
                self.nodesocket.send(self.data.encode('utf-8'))
                # time.sleep(1)
                data=self.nodesocket.recv(256)

            if "Mapping" in data.decode('utf-8'):
                maps_completed+=1
                # print("Maps Completed", maps_completed)

                sent_key=self.nodesocket.recv(1048576)
                self.sent_key=sent_key.decode()

                if maps_completed==self.no_of_mappers:
                    print(datetime.now(),"| "+"Complete Mapping Completed")
                
                # print("Master Thread ", str(self.nodesocket))
            
            elif "Reducing" in data.decode('utf-8'):
                reds_completed+=1
                # print("Reducers Completed", reds_completed)

                if reds_completed==self.no_of_reducers:
                    print(datetime.now(),"| "+" Complete Reducction Completed",reds_completed)
                
                # print("Master Thread ", str(self.nodesocket))
        

        # def join(self):
        #     threading.Thread.join(self)
        #     return self._return

    def activate_master_node(self):

        print(datetime.now(),"| "+"Master Started")
        master_socket = socket(AF_INET, SOCK_STREAM)
        master_socket.bind((self.master_adrress,self.master_port))
        mappers=threading.Thread(target=self.activate_map_process,args=[], daemon=True)
        mappers.start()
        ## Get File Size

        if self.function=="wordcount":
            # Open the file and get the line count
            with open(self.filelocations[0], 'r') as fp:
                no_of_lines= len(fp.readlines())

            thread_id=0
            #These many lines will be sent to each mapper for mapping
            val=no_of_lines//self.no_of_mappers
            threads_for_mappers=[]

            # Our master node will listen to any inomcing connections and then creates a new thread for each of the connections to maintain concurrency
            while True:
                
                # Get data ready: the moment we get a connection we will create  new thread and send the data to the mappers
                with open(self.filelocations[0], 'r') as fp:
                    fp.seek(thread_id*val)
                    data= fp.readlines()[thread_id*val:(thread_id+1)*val]
                    data= " ".join(data)

                # Listen and Accept
                master_socket.listen()
                nodesocket, nodeaddress = master_socket.accept()

                # Create a new thread
                newmasterthread = self.masterthread(nodeaddress, nodesocket,thread_id,data, self.no_of_mappers, self.no_of_reducers,self.function)
                threads_for_mappers.append(newmasterthread)
                newmasterthread.start()

                #Updating the thread id and once we reach the mapper count then we can stop listenting 
                # print("---------------------",thread_id)
                thread_id+=1
                if thread_id==self.no_of_mappers:
                    break
            

        elif  self.function=="invertedindex":
            thread_id=0
            #These many lines will be sent to each mapper for mapping
            threads_for_mappers=[]

            # Our master node will listen to any inomcing connections and then creates a new thread for each of the connections to maintain concurrency
            while True:
                print(self.filelocations[thread_id])
                # time.sleep(1)
                # Get data ready: the moment we get a connection we will create  new thread and send the data to the mappers
                with open(self.filelocations[thread_id], 'r') as fp:
                    data= fp.readlines()
                    data= " ".join(data)

                # Listen and Accept
                master_socket.listen(1)
                nodesocket, nodeaddress = master_socket.accept()

                # Create a new thread
                newmasterthread = self.masterthread(nodeaddress, nodesocket,thread_id,data, self.no_of_mappers, self.no_of_reducers,self.function, self.filelocations[thread_id])
                threads_for_mappers.append(newmasterthread)
                newmasterthread.start()

                #Updating the thread id and once we reach the mapper count then we can stop listenting 
                # print("---------------------",thread_id)
                thread_id+=1
                if thread_id==self.no_of_mappers:
                    break
                
        # Our threads will get the confirmation from our mappers  and this will act as barrier ( Barrier implemetaion is also present in mappers)
        for thrd in threads_for_mappers:
            thrd.join()


        # Get  all the keys 
        # print("___....____"*30)
        key_set= set()
        i=0
        for thrd in threads_for_mappers:
            try:
                sent_key=thrd.sent_key.split()
                key_set = key_set.union(sent_key)
            except:
                pass
                # print("->"*100)

        key_list= list(key_set)
        # print(len(key_list))
        val= len(key_list)//self.no_of_reducers

        thread_id=0
        reducers=threading.Thread(target=self.activate_reduce_process,args=[], daemon=True)
        reducers.start()
        threads_for_reducers=[]
        while True:
            
            data= " ".join(key_list[thread_id*val:(thread_id+1)*val])
            master_socket.listen(1)
            nodesocket, nodeaddress = master_socket.accept()
            newmasterthread = self.masterthread(nodeaddress, nodesocket,thread_id,data, self.no_of_mappers, self.no_of_reducers, self.function)
            threads_for_reducers.append(newmasterthread)
            newmasterthread.start()
            # print("---------------------",thread_id)
            thread_id+=1
            if thread_id==self.no_of_reducers:
                break

        for thrd in threads_for_reducers:
            thrd.join()
        
        # print("___....____"*30)
        mastertokvsocket = socket(AF_INET, SOCK_STREAM)
        mastertokvsocket.connect((self.kv_address,self.kv_port))

        print(datetime.now(),"| "+"Generating output")
        if self.function=="wordcount":
            mastertokvsocket.send("generatewcoutput".encode())
        elif self.function=="invertedindex":
            mastertokvsocket.send("generateinvindoutput".encode())




if __name__ == '__main__':
    # config_file=input("Please enter the location of the location of Config file \n")
    config_file=input("Please enter the name of your config file \n")
    newmp= MapReduce(config_file)
    maps_completed=0
    reds_completed=0
    newmp.activate_master_node()
    

    

    
            
