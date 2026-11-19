import socket 
import threading
import time
import subControl
import eventCast
import util
import configParser
import mylock

# ==================================================
# PUBLISHER
# ==================================================

class Publisher:

    def __init__(self, config:configParser.ConfigPublisher):
        #DADOS
        self.id:int = config["id"]
        self.inicio:float = 0.0
        #Vamos assumir que é (ip,porta)
        self.broker_control : tuple[str,int] = config["broker_control"]
        self.broker_data : tuple[str,int] = (config["broker_control"][0],-1)
        self.log :str = config["log"]
        self.start_after :int= config["start_after"]
        #Vamos assumir que publish é uma lista de (topico, QoS, count, rate)
        self.publish : list[tuple[str,int,int,int]]= config["publish"]
        self.drain: int= config["drain"]
        #ESTADO
        self.runningTCP:int = len(self.publish)
        #SOCKET
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        #LOCKS
        self.runningTCPLock = mylock.Lock()
        self.control_socketLock = mylock.Lock()
        self.logLock = mylock.Lock()
        
    # ==================================================
    # CICLO PRINCIPAL
    # ==================================================
 
    def executar(self):
        thread_list: list[threading.Thread] = []
        try:
            #Enviar a primeira mensagem com EstablishConnection para receber o id
            self.control_socket.connect(self.broker_control)
            self.inicio = time.monotonic()
            mensagem = subControl.controiMessage(True,True,True,False,False,self.publish[0][1],self.id,self.publish[0][0],0)
            self.control_socket.sendall(mensagem)
            resposta = subControl.recebeMensagemSubControl(self.control_socket)
            tamanhoTotal, es, ps, rs, cs, to, QoS, identificador, porta, topico = subControl.interpretar_SubControl(resposta)
            self.broker_data = (self.broker_data[0],porta)
            thread = threading.Thread(target=self.cicloUDP,args=(self.publish[0][0],self.publish[0][1],self.publish[0][2],self.publish[0][3]))
            thread.start()
            thread_list.append(thread)
            self.runningTCP -=1
            #Enviar caso tenha mais registo de publicacao para fazer envia uma de cada vez, sinalizando qual a última atraves do valor de close
            for i in range(1,len(self.publish)): 
                thread = threading.Thread(target=self.cicloPublicacao,args=(self.publish[i][0],self.publish[i][1],self.publish[i][2],self.publish[i][3]))
                thread.start()
                thread_list.append(thread)

            while (True):
                with self.runningTCPLock.read:
                    if self.runningTCP <=0: break
                time.sleep(1)
            util.fechar(self.control_socket)

        except Exception as e:
            print(f"Erro: {e}")
        
        finally:
            if self.runningTCP>0: util.fechar(self.control_socket)
            else: 
                for thread in thread_list:
                    thread.join()

    # ==================================================
    # FUNCAO DE THREAD - FAZ SUBSCRICAO/CICLO_UDP
    # ==================================================

    def cicloPublicacao(self,topico:str,qos:int,count:int,rate:int):
        try:
            mensagem = subControl.controiMessage(False,True,True,False,False,qos,self.id,topico,0)
            with self.control_socketLock.write:
                self.control_socket.sendall(mensagem)
            with self.runningTCPLock.write:
                self.runningTCP -=1
            self.cicloUDP(topico,qos,count,rate)
        except OSError:
            raise

    # ==========================================================
    # CICLO_UDP - RESPONSAVEL POR ENVIAR METRICAS COM THREADS
    # ==========================================================

    def cicloUDP(self,topico:str,qos:int,count:int,rate:int):
        data_socket = None
        parar = threading.Event()
        message_id = 1
        thread_ack = None
        try:
            #Uma opção é cada thread teria o seu socket e recebia o seu ack do message_id
            data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            acks_lock:mylock.Lock = mylock.Lock()
            acks:dict[int,bool] = {}
            time.sleep(max(self.start_after-time.monotonic()+self.inicio,0))
            thread_ack = threading.Thread(target=self.recebeAcks,args=(acks,data_socket,acks_lock,parar))
            thread_ack.start()
            while (count>0):
                thread_send = threading.Thread(target=self.enviaMetrica,args=(topico,acks,data_socket,message_id,qos,acks_lock))
                thread_send.start()
                count -= 1
                message_id += 1
                time.sleep(1/rate)
            espera = time.monotonic()
            while (time.monotonic()-espera < self.drain):
                cond = True
                with acks_lock.read:
                    for valor in acks.values():
                        if (valor==False): cond = False
                if (cond): break
                time.sleep(0.5)

        except OSError:
            pass
        finally:
            parar.set()
            if data_socket is not None: util.fechar(data_socket)
            if thread_ack is not None: thread_ack.join()

    # ==================================================================
    # FUNCAO DE THREAD - ENVIA UMA METRICA E RETRANSMITE SE NECESSÁRIO
    # ==================================================================

    def enviaMetrica(self,topico:str,acks:dict[int,bool],socket:socket.socket,message_id:int,qos:int, lock:mylock.Lock):
        addr = self.broker_data
        message = eventCast.constroiMessage(False,qos,message_id,self.id,topico,"")
        with lock.write:
            acks[message_id]=False
        socket.sendto(message,addr)
        with self.logLock.write:
            util.registaLog(self.log, self.id, topico, message_id)
        inicio = time.monotonic()
        while time.monotonic()-inicio < self.drain:
            time.sleep(0.5)
            with lock.read:
                if acks[message_id]:
                    break
            try:
                socket.sendto(message,addr)
            except OSError:
                break

    # ============================================================
    # FUNCAO DE THREAD - RESPONSAVEL POR REGISTAR ACKS RECEBIDOS
    # ============================================================

    def recebeAcks(self,acks:dict[int,bool],my_socket:socket.socket, lock:mylock.Lock, parar: threading.Event):
        time.sleep(max(self.start_after-time.monotonic()+self.inicio,0))
        my_socket.settimeout(1.0)
        while not parar.is_set():
            try:
                resposta, addr = my_socket.recvfrom(1024)
                try:
                    ack, qos, message_id, identificador, topico, metrica = eventCast.interpretarEventCast(resposta)
                except Exception:
                    continue
                if ack:
                    with lock.write:
                        if message_id in acks: acks[message_id] = True
            except socket.timeout:
                continue
            except OSError:
                break
    




    


    