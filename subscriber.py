import socket
import threading
import time
import util
import eventCast
import subCast

class Publisher:

    def __init__(self, config):
        self.id = -1
        self.inicio = 0.0
        #Vamos assumir que é (ip,porta)
        self.broker_control = config["broker_control"]
        self.broker_data = (config["broker_control"][0],-1)
        self.log = config["log"]
        self.duration = config["duration"]
        #Vamos assumir que subscribe é uma lista de (filtro, QoS)
        self.subscribe = config["subscribe"]
        #Vamos assumir que é uma lista de (tempo,True=subscribe ou false=unsubscribe (1 ou 0), topico, qos (apenas usado no True)) neste momento não dividimos mas depois pode ser dividido
        self.actions = config["at"]
        self.running = True
        self.messages = {}
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.data_socket.bind(('', 0))
        self.data_port = self.data_socket.getsockname()[1]

    def executar(self):
            try:
                self.inicio = time.monotonic()
                #Enviar a primeira mensagem com EstablishConnection para receber o id
                self.control_socket.connect(self.broker_control)
                mensagem = subCast.controiMessage(True,False,True,False,False,self.subscribe[0][1],False,0,self.subscribe[0][0],self.data_port)
                self.control_socket.sendall(mensagem)
                resposta = subCast.recebeMensagemSubCast(self.control_socket)
                tamanhoTotal, es, ps, rs, cs, to, QoS, close, identificador, porta, topico = subCast.interpretar_SubCast(resposta)
                self.id = identificador
                self.broker_data[1] = porta
                #Enviar caso tenha mais registo de publicacao para fazer envia uma de cada vez, sinalizando qual a última atraves do valor de close
                for subscricao in self.subscribe[1:]: 
                    mensagem = subCast.controiMessage(False,True,True,False,False,subscricao[1],False,self.id,subscricao[0],self.data_port)
                    self.control_socket.sendall(mensagem)
                    resposta = subCast.recebeMensagemSubCast(self.control_socket)
                thread_list = []
                #Cria uma thread para cada ação
                for acao in self.actions:
                    thread = threading.Thread(target=self.acaoProgramada,args=(acao[0],acao[1],acao[2],acao[3])) 
                    thread_list.append(thread)
                    thread.start()
                #Inicia a thread que recebe as metricas
                thread_UDP = threading.Thread(target=self.cicloUDP)
                thread_UDP.start()
                #Adormece até ao momento de terminar e depois termina
                time.sleep(self.duration-time.monotonic()+self.inicio)
                self.running=False

            except Exception as e:
                print(f"Erro: {e}")
            
            finally:
                self.data_socket.close()
                self.control_socket.close()
                thread_UDP.join()
                for thread in thread_list:
                    thread.join()

    def acaoProgramada(self,tempo,tipo,topico,qos):
        try:
            #Constroi a mensagem dependendo se é de subscricao ou publicacao
            mensagem = subCast.controiMessage(False,False,True,False,False,qos,False,self.id,topico,self.data_port) if tipo else subCast.controiMessage(False,False,False,True,False,0,False,self.id,topico,self.data_port)
            #Adormece a thread até que esteja no momento de atuar
            time.sleep(tempo-time.monotonic()+self.inicio)
            #Envia a mensagem e recebe resposta
            self.control_socket.sendall(mensagem)
            resposta = subCast.recebeMensagemSubCast(self.control_socket)
            if tipo: self.subscribe.append((topico,qos))
            else: self.subscribe = [(filtro, nivel_qos) for filtro, nivel_qos in self.subscribe if filtro != topico]
        except OSError as e:
            raise e

    def cicloUDP(self):
        while self.running:
            try:
                mensagem, addr = self.data_socket.recvfrom(2048)
                thread = threading.Thread(target=self.tratamentoEventCast,args=(mensagem, addr))
                thread.start()
            except OSError as e:
                if self.running: self.running=False
                raise e

    def tratamentoEventCast(self,mensagem,addr):
        ack, qos, message_id, identificador, topico, metrica = eventCast.interpretar_EventCast(mensagem)
        self.enviaACKEventCast(message_id,addr)
        self.regista_mensagem(message_id,identificador,topico,metrica,qos)
        
        return

    def enviaACKEventCast(self,message_id, addr):
        ack_message = eventCast.constroiAckMessage(message_id,self.id)
        try:
            self.data_socket.sendto(ack_message,addr)
        except OSError as e:
            return

    def regista_mensagem(self,identificador,message_id,topico,metrica,qos):
        try:
            qosEfetivo = self.encontraQoS(topico,qos)
            regiao, estacao, metrica = topico.split("/")
            set = self.messages.setdefault(regiao, {}).setdefault(estacao, {}).setdefault(metrica, set())
            if qosEfetivo == 2 and message_id in set: return
            util.registaLog(self.log, identificador, topico, message_id)
            set.add(message_id)
        except Exception as e:
            return

    def encontraQoS(self,topico,qos):
        if qos == 1: return qos
        for filtro,filtro_qos in self.subscribe:
            if (util.filtroCorresponde(filtro,topico)) and filtro_qos == 2: return 2
        return qos

   

    

    
    