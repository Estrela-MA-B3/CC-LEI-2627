import socket 
import threading
import time
import subCast
import eventCast
import util


class Publisher:

    def __init__(self, config):
        self.id = -1
        self.inicio = 0.0
        #Vamos assumir que é (ip,porta)
        self.broker_control = config["broker_control"]
        self.broker_data = (config["broker_control"][0],-1)
        self.log = config["log"]
        self.start_after = config["start_after"]
        #Vamos assumir que publish é uma lista de (topico, QoS, count, rate)
        self.publish = config["publish"]
        self.drain = config["drain"]
        self.runningTCP = True
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        
        
    def executar(self):
        try:
            #Enviar a primeira mensagem com EstablishConnection para receber o id
            self.control_socket.connect(self.broker_control)
            close = True if len(self.publish)==1 else False
            mensagem = subCast.controiMessage(True,True,True,False,False,self.publish[0][1],close,0,self.publish[0][0],0)
            self.control_socket.sendall(mensagem)
            resposta = subCast.recebeMensagemSubCast(self.control_socket)
            tamanhoTotal, es, ps, rs, cs, to, QoS, close, identificador, porta, topico = subCast.interpretar_SubCast(resposta)
            self.id = identificador
            self.broker_data[1] = porta
            self.inicio = time.monotonic()
            #Enviar caso tenha mais registo de publicacao para fazer envia uma de cada vez, sinalizando qual a última atraves do valor de close
            for i in range(1,len(self.publish)): 
                close = True if (i==len(self.publish)-1) else False
                mensagem = subCast.controiMessage(False,True,True,False,False,self.publish[i][1],close,self.id,self.publish[i][0],0)
                self.control_socket.sendall(mensagem)
                resposta = subCast.recebeMensagemSubCast(self.control_socket)
            self.control_socket.close()
            self.runningTCP = False
            thread_list = []
            for publicacao in self.publish:
                udp_thread = threading.Thread(target=self.cicloUDP,args=(publicacao[0],publicacao[1],publicacao[2],publicacao[3]))
                thread_list.append(udp_thread)
                udp_thread.start()
            
        except Exception as e:
            print(f"Erro: {e}")
        
        finally:
            if self.runningTCP: self.control_socket.close()
            else: 
                for thread in thread_list:
                    thread.join()

    def cicloUDP(self,topico,QoS,count,rate):
        try:
            message_id = 1
            #Uma opção é cada thread teria o seu socket e recebia o seu ack do message_id
            data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            acks = {}
            time.sleep(self.start_after-time.monotonic()+self.inicio)
            thread_ack = threading.Thread(target=self.recebeAcks,args=(acks,data_socket))
            thread_ack.start()
            while (count>0):
                thread_send = threading.Thread(target=self.enviaMetrica,args=(topico,acks,data_socket,message_id))
                thread_send.start()
                count -= 1
                time.sleep(rate)
        finally:
            data_socket.close()

    def enviaMetrica(self,topico,acks,socket,message_id):
        addr = self.broker_data
        message = eventCast.constroiMessage(topico,message_id)
        acks[message_id]=False
        socket.sendto(message,addr)
        inicio = time.monotonic()
        util.registaLog(self.log, self.id, topico, message_id)
        while (not acks[message_id] and time.monotonic()-inicio < self.drain):
            time.sleep(1)
            socket.sendto(message,addr)
        return

    def recebeAcks(self,acks,socket):
        resposta, addr = socket.recvfrom(1024)
        ack, qos, message_id, identificador, topico, metrica = eventCast.interpretar_EventCast(resposta)
        if ack and message_id in acks: acks[message_id] = True

    




    


    