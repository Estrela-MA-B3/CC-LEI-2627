import socket
import threading
import time
import util
import eventCast
import subControl
import configParser
import mylock

# ==================================================
# SUBSCRIBER
# ==================================================

class Subscriber:

    def __init__(self, config:configParser.ConfigSubscriber):
        #DADOS
        self.id:int = config["id"]
        self.inicio = 0.0
        #Vamos assumir que é (ip,porta)
        self.broker_control: tuple[str, int] = config["broker_control"]
        self.broker_data: tuple[str, int] = (config["broker_control"][0],-1)
        self.log:str = config["log"]
        self.duration: int = config["duration"]
        #Vamos assumir que subscribe é uma lista de (filtro, QoS)
        self.subscribe : list[tuple[str, int]] = config["subscribe"]
        #Vamos assumir que é uma lista de (tempo,True=subscribe ou false=unsubscribe (1 ou 0), topico, qos (apenas usado no True)) neste momento não dividimos mas depois pode ser dividido
        self.actions: list[tuple[int, bool, str, int]] = config["at"]
        self.messages:dict[str,dict[str,dict[str,tuple[mylock.Lock,set[int]]]]] = {}
        #ESTADO
        self.running = True
        #SOCKET
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        #LOCKS
        self.runningLock = mylock.Lock()
        self.control_socketLock = mylock.Lock()
        self.messagesLock = mylock.Lock()
        self.subscribeLock = mylock.Lock()
        self.logLock = mylock.Lock()

    # ==================================================
    # CICLO PRINCIPAL
    # ==================================================

    def executar(self):
            thread_list:list[threading.Thread] = []
            data_socket:socket.socket
            self.inicio = time.monotonic()
            try:
                #Enviar a primeira mensagem e recebe a mensagem para receber qual a portaUDP do broker
                data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                data_socket.bind(('', 0))
                data_port = data_socket.getsockname()[1]
                self.control_socket.connect(self.broker_control)
                mensagem = subControl.controiMessage(True,False,True,False,False,self.subscribe[0][1],self.id,self.subscribe[0][0],data_port)
                self.control_socket.sendall(mensagem)
                resposta = subControl.recebeMensagemSubControl(self.control_socket)
                tamanhoTotal, udp, ps, rs, cs, to, QoS, identificador, porta, topico = subControl.interpretar_SubControl(resposta)
                self.broker_data = (self.broker_data[0],porta)
                thread = threading.Thread(target=self.cicloUDP,args=(data_socket,)) 
                thread_list.append(thread)
                thread.start()
                #Caso tenha mais subscricoes estas são feitas paralelamente em varias threads
                for subscricao in self.subscribe[1:]:
                    thread = threading.Thread(target=self.acaoProgramada,args=(0,True,subscricao[0],subscricao[1])) 
                    thread_list.append(thread)
                    thread.start()
                #Cria uma thread para cada ação
                for acao in self.actions:
                    thread = threading.Thread(target=self.acaoProgramada,args=(acao[0],acao[1],acao[2],acao[3])) 
                    thread_list.append(thread)
                    thread.start()
                #Adormece até ao momento de terminar e depois termina
                time.sleep(self.duration-time.monotonic()+self.inicio)
                with self.runningLock.write:
                    self.running=False

            except Exception as e:
                print(f"Erro: {e}")
            
            finally:
                self.control_socket.close()
                for thread in thread_list:
                    thread.join()

    # ===================================================
    # FUNCAO DE THREAD - CRIA SOCKET/SUBSCREVE/CICLO_UDP
    # ===================================================

    def acaoProgramada(self,tempo :int,tipo : bool,topico : str,qos : int):
        data_socket = None
        try:
            #Constroi a mensagem dependendo se é de subscricao ou publicacao
            data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            data_socket.bind(('', 0))
            data_port = data_socket.getsockname()[1]
            rs = tipo
            cs = not tipo
            mensagem = subControl.controiMessage(True,False,rs,cs,False,qos,self.id,topico,data_port) 
            #Adormece a thread até que esteja no momento de atuar
            time.sleep(max(tempo-time.monotonic()+self.inicio,0))
            #Envia a mensagem e recebe resposta
            with self.control_socketLock.write:
                self.control_socket.sendall(mensagem)
            if tempo:
                if tipo: 
                    with self.subscribeLock.write:
                        self.subscribe.append((topico,qos))
                else: 
                    with self.subscribeLock.write:
                        self.subscribe = [(filtro, nivel_qos) for filtro, nivel_qos in self.subscribe if filtro != topico]
            self.cicloUDP(data_socket)
        except OSError:
            pass
        finally:
            if data_socket is not None: data_socket.close()

    # ================================================================================
    # CICLO_UDP - RESPONSAVEL POR RECEBER METRICAS E CRIAR THREADS PARA O TRATAMENTO
    # ================================================================================

    def cicloUDP(self, data_socket:socket.socket):
        data_socket.settimeout(1.0)
        while True:
            with self.runningLock.read:
                if not self.running: break
            try:
                mensagem, addr = data_socket.recvfrom(2048)
                thread = threading.Thread(target=self.tratamentoEventCast,args=(mensagem, addr,data_socket))
                thread.start()
            except socket.timeout:
                pass
            except OSError as e:
                raise e

    # ===================================================
    # RECEBE UMA MENSAGEM COM METRICA E FAZ O TRATAMENTO 
    # ===================================================

    def tratamentoEventCast(self,mensagem:bytes,addr:tuple[str,int],data_socket:socket.socket):
        try:
            ack, qos, message_id, identificador, topico, metrica = eventCast.interpretarEventCast(mensagem)
            ack_message = eventCast.constroiAckMessage(message_id,self.id,topico)
            data_socket.sendto(ack_message,addr)
            self.regista_mensagem(self.id,message_id,topico,metrica,qos)
        except OSError:        
            return
        
    # ==================================================
    # REGISTA UMA MENSAGEM NA LOG - VERIFICA QOS
    # ==================================================

    def regista_mensagem(self,identificador:int,message_id:int,topico:str,metricaRecebida:str,qos:int):
        try:
            qosEfetivo = self.encontraQoS(topico,qos)
            set_registo = None
            regiao, estacao, metrica = topico.split("/")
            with self.messagesLock.read:
                if (regiao in self.messages and estacao in self.messages[regiao] and metrica in self.messages[regiao][estacao]):
                    set_registo = self.messages[regiao][estacao][metrica]
            if set_registo is None: 
                with self.messagesLock.write:
                    if (regiao in self.messages and estacao in self.messages[regiao] and metrica in self.messages[regiao][estacao]):
                        set_registo = self.messages[regiao][estacao][metrica]
                    else: 
                        set_registo = self.messages.setdefault(regiao, {}).setdefault(estacao, {}).setdefault(metrica, (mylock.Lock(),set()))
            if qosEfetivo == 2:
                with set_registo[0].read:
                    if (message_id in set_registo[1]): return
                with set_registo[0].write:
                    if(message_id in set_registo[1]): return
                    set_registo[1].add(message_id)
            with self.logLock.write:
                util.registaLog(self.log, identificador, topico, message_id)
                    
        except Exception:
            return

    def encontraQoS(self,topico:str,qos:int):
        if qos == 1: return qos
        with self.subscribeLock.read:
            lista = list(self.subscribe)
        for filtro,filtro_qos in lista:
            if (util.filtroCorresponde(filtro,topico)) and filtro_qos == 2: return 2
        return qos

   

    

    
    