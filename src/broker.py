
import socket
import threading
import time
import subControl
import eventCast
import configParser
import mylock

# ==================================================
# CODIGOS DE RETORNO
# ==================================================

SUCESSO = 0
TOPICO_JA_EXISTE = -1
SUBSCRICAO_ATUALIZADA = -2
ERRO_TOPICO_INVALIDO = -1
ERRO_TOPICO_INEXISTENTE = -2
ERRO_SUBSCRICAO_INEXISTENTE = -3


# ==================================================
# ESTRUTURAS DA ARVORE
# ==================================================

class RegionNode:
    def __init__(self):
        # Subscritores de "regiao/*"
        self.subscribers:dict[int,int] = {}  # {subscriber_id: qos}
        self.children:dict[str,StationNode] = {}     # {estacao: StationNode}


class StationNode:
    def __init__(self):
        # Subscritores de "regiao/estacao/*"
        self.subscribers:dict[int,int] = {}  # {subscriber_id: qos}
        self.children:dict[str,TopicLeaf] = {}     # {metrica: TopicLeaf}


class TopicLeaf:
    def __init__(self, qos:int):
        # QoS definida no registo do topico
        self.qos = qos
        # Message_id -> {subscriber_id: (timestamp)}
        self.messages:dict[int, tuple[mylock.Lock,dict[int,float]]] = {}
        # Subscritores do topico especifico
        self.subscribers:dict[int,int] = {}  # {subscriber_id: qos}
        

# ==================================================
# BROKER
# ==================================================

class Broker:

    def __init__(self, broker_id:int, config:configParser.ConfigBroker):
        #DADOS
        self.id = broker_id
        self.bind = config["bind"]
        self.control_port = config["control_port"]
        self.data_port = config["data_port"]
        self.duration = config["duration"]
        self.evic_after = config["evict_after"]
        # Dicionário onde vou manter os tópicos: Região -> Estação -> Métrica -> Dict(Subscribers,Qos)/Qos Publisher/Acks: Dicionario : Message_id -> (Lista de Waiting Acks)
        #Usar a presença de Message_id para verificar se a mensagem já foi enviada uma vez, isto é, se o broker já recebeu aquele message_id, ao procurar pelos subscritores a mandar, só recolhe os que tem QoS = 1 
        self.topics:dict[str,RegionNode] = {}
        # Dicionário de subscriber id -> (Endereço,{topico: portaUDP})
        self.subscritores:dict[int,tuple[str,dict[str,int]]] = {}
        # Dicionário de publisher id -> Endereço
        self.publishers:dict[int,set[str]] = {}
        # Tuplo com os sockets de publishers e subscritores 
        self.sockets:tuple[dict[int,socket.socket],dict[int,socket.socket]] = ({},{})

        #ESTADO 
        self.running:bool = True
        self.erro_tcp = None
        self.erro_udp = None

        #SOCKETS
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

        #LOCKS
        self.topicLock = mylock.Lock()
        self.subscritoresLock = mylock.Lock()
        self.publishersLock = mylock.Lock()
        self.socketsLock = mylock.Lock()


    # ==================================================
    # CICLO PRINCIPAL
    # ==================================================

    def executar(self):
        
        self.running = True
        self.erro_tcp = None
        self.erro_udp = None
        tcp_thread = None 
        udp_thread = None

    
        try:

            self.control_socket.bind((self.bind, self.control_port))
            self.control_socket.listen()
            self.data_socket.bind((self.bind, self.data_port))

            tcp_thread = threading.Thread(target=self.cicloTCP)
            udp_thread = threading.Thread(target=self.cicloUDP)

            tcp_thread.start()
            udp_thread.start()
    
            inicio = time.monotonic()
    
            while self.running:
                if self.erro_tcp is not None:
                    print(f"Erro na escuta TCP: {self.erro_tcp}")
                    break

                if self.erro_udp is not None:
                    print(f"Erro na escuta UDP: {self.erro_udp}")
                    break
    
                if time.monotonic() - inicio >= self.duration:
                    break
    
                time.sleep(1)
    
        except Exception as e:
            print(f"Erro no broker: {e}")
    
        finally:
            self.running = False
            self.control_socket.close()
            self.data_socket.close()
            self.fecha_sockets()
    
            if tcp_thread is not None: tcp_thread.join()
            if udp_thread is not None: udp_thread.join()

    # ==================================================
    # FUNÇÃO DE CICLO DOS SOCKETS
    # ==================================================
    
    
    def cicloTCP(self):
        self.control_socket.settimeout(1.0)
        while self.running:
            try:
                client_socket, client_address = self.control_socket.accept()
    
                thread = threading.Thread(target=self.tratamentoSubControl,args=(client_socket, client_address))
                thread.start()

            #Termina com o close e a excessão associada caso contrário colocamos o timeout e esta condição para ele avaliar o running periodicamente
            except socket.timeout:
                continue
            except OSError as e:
                if self.running:
                    self.erro_tcp = e
                break    
    
    def cicloUDP(self):
        self.data_socket.settimeout(1.0)
        while self.running:
            try:
                mensagem, addr = self.data_socket.recvfrom(1024)
    
                thread = threading.Thread(target=self.tratamentoEventCast,args=(mensagem, addr),daemon=True)
                thread.start()
            except socket.timeout:
                continue
            except OSError as e:
                if self.running:
                    self.erro_udp = e
                break

    # ==================================================
    # TRATAMENTO DE UMA MENSAGEM RECEBIDA
    # ==================================================

    def tratamentoSubControl(self, client_socket:socket.socket, client_address:tuple[str,int]):
        id = -1
        tipo = 0
        try:
            buffer = subControl.recebeMensagemSubControl(client_socket)
            tamanhoTotal, udp, ps, rs, cs, to, QoS, identificador, porta, topico = subControl.interpretar_SubControl(buffer)
            tipo = 0 if ps else 1
            id = identificador
            with self.socketsLock.write:
                self.sockets[tipo][id] = client_socket
            while True:
                if (rs and topico):
                    if (ps): self.registoPub(identificador,topico,QoS)
                    else: 
                        self.subscricaoSub(identificador,topico,QoS,porta,client_address[0])
                if (cs): self.cancelarSubscricao(identificador,topico)
                if (to): 
                    self.terminacaoOrdenada(ps,identificador)
                    break
                # So enviamos uma mensagem se estiver a pedir a porta UDP ou se for uma subscricao para comunicar o qos
                if (udp): self.mensagemSubControl(client_socket,self.id)
                # Se foi gerada uma terminacao ordenada por parte do broker ao publisher ou ao subscriber ele deve terminar
                with self.subscritoresLock.read:
                    if (not ps and (not identificador in self.subscritores)):break
                with self.publishersLock.read: 
                    if (ps and (not identificador in self.publishers) or to):break
                buffer = subControl.recebeMensagemSubControl(client_socket)
                tamanhoTotal, udp, ps, rs, cs, to, QoS, identificador, porta, topico = subControl.interpretar_SubControl(buffer) 
        except Exception:
            pass
        finally:
            if id == -1: 
                client_socket.close()
            else: 
                with self.socketsLock.write: 
                    socket = self.sockets[tipo].pop(id,None)
                    if socket is not None: 
                        socket.close()

    def mensagemSubControl(self, client_socket:socket.socket,identificador:int):
        try:
            ack_message = subControl.constroiAckMessage(identificador,self.data_port)
            client_socket.sendall(ack_message)
        except OSError as e:
            raise e

    def tratamentoEventCast(self, mensagem:bytes, addr:tuple[str,int]):
        try: 
            ack, qos, message_id, identificador, topico, metrica = eventCast.interpretarEventCast(mensagem)
        except Exception:
            return
        # Estamos a receber um ACK de um subscritor tem de ter o message_id, identificador e topico, neste caso, metrica = null
        if (ack):
            ack_dic = None
            partes = topico.split("/")
            if len(partes) != 3 or any(not parte or parte == "*" for parte in partes):return
            with self.topicLock.read:
                if partes[0] in self.topics:
                    if partes[1] in self.topics[partes[0]].children:
                        if partes[2] in self.topics[partes[0]].children[partes[1]].children:
                            if message_id in self.topics[partes[0]].children[partes[1]].children[partes[2]].messages:
                                ack_dic = self.topics[partes[0]].children[partes[1]].children[partes[2]].messages[message_id]
            if ack_dic is not None: 
                with ack_dic[0].write:
                    ack_dic[1].pop(identificador, None)

        # Estamos a receber uma nova métrica de um publisher. Neste caso o identificador é o do publisher que acaba por ser também inutil
        else:
            thread_ack = threading.Thread(target=self.enviaACKEventCast,args=(message_id,addr))
            thread_ack.start()
            #Operação com Lock para ver quem são os destinatários a enviar!
            sub_dic, ack_dic = self.obterDestinatarios(topico,message_id)
            if sub_dic is None or ack_dic is None: return
            for id_sub,endereco in sub_dic.items():
                self.enviaPublicacao(endereco,mensagem)
                with ack_dic[0].write:
                    ack_dic[1][id_sub] = (time.monotonic())
            while True:
                time.sleep(1)
                lista = None
                with ack_dic[0].read:
                    lista = list(ack_dic[1].items())
                if not lista: break
                for id_sub, tempo in lista:
                        evicted = False
                        # Verifica se está no momento de fazer evict 
                        if time.monotonic() - tempo > self.evic_after:
                            thread = threading.Thread(target=self.terminacaoOrdenada,args=(False, id_sub))
                            thread.start()
                            evicted=True
                        #Verifica se ele foi terminado por completo
                        else: 
                            with self.subscritoresLock.read:
                                if (id_sub not in self.subscritores):
                                    evicted = True
                        # Caso um dos testes anteriores seja positivo não é feita nova retransmissão                   
                        if evicted:
                            with ack_dic[0].write:
                                ack_dic[1].pop(id_sub,None)
                        else: self.enviaPublicacao(sub_dic[id_sub],mensagem)
            thread_ack.join()
        return
    
        
    def enviaPublicacao(self,endereco:tuple[str,int],mensagem:bytes):
        try:
            self.data_socket.sendto(mensagem,endereco)
        except OSError as e:
            return

    def enviaACKEventCast(self,message_id:int, addr:tuple[str,int]):
        ack_message = eventCast.constroiAckMessage(message_id,self.id,"")
        try:
            self.data_socket.sendto(ack_message,addr)
        except OSError as e:
            return

    # ==================================================
    # REGISTO DE PUBLICACAO / TOPICO
    # ==================================================

    def registoPub(self,publisher_id:int, topico:str, qos:int)->int:
        partes = topico.split("/")
        if len(partes) != 3 or any(not parte or parte == "*" for parte in partes):
            return ERRO_TOPICO_INVALIDO
        
        regiao, estacao, metrica = partes

        # Insere na arvore todas as estruturas que é preciso criar 
        with self.topicLock.write:
                node = self.topics.setdefault(regiao, RegionNode()).children.setdefault(estacao, StationNode()).children.setdefault(metrica, TopicLeaf(qos))
                node.qos = qos
            
        
        with self.publishersLock.write:
            self.publishers.setdefault(publisher_id, set()).add(topico)

        return SUCESSO 

    # ==================================================
    # SUBSCRICAO
    # ==================================================

    def subscricaoSub(self, subscriber_id:int, topico:str, qos:int, portaUDP:int, endIP:str)->int:
        partes = topico.split("/")
        
        # Subscrição com formato inválido 
        if len(partes) < 2 or len(partes)>3:
            return -1

        # Faz a subscrição
        if len(partes) == 2 and partes[1] == "*":
            with self.topicLock.write:
                self.topics.setdefault(partes[0], RegionNode()).subscribers[subscriber_id] = qos

        if len(partes) == 3:
            if partes[2] == "*":
                with self.topicLock.write:
                    self.topics.setdefault(partes[0], RegionNode()).children.setdefault(partes[1],StationNode()).subscribers[subscriber_id] = qos
            else:
                with self.topicLock.write:
                    self.topics.setdefault(partes[0], RegionNode()).children.setdefault(partes[1],StationNode()).children.setdefault(partes[2],TopicLeaf(0)).subscribers[subscriber_id] = qos

        with self.subscritoresLock.write:
            #Atualiza as subscricoes do subscriber 
            tuplo = self.subscritores.setdefault(subscriber_id, (endIP,{}))
            tuplo[1][topico] = portaUDP
    
        return 0

    # ==================================================
    # CANCELAMENTO DE SUBSCRICAO
    # ==================================================
    def cancelarSubscricao(self, subscriber_id:int, topico:str)->int:
        partes = topico.split("/")

        # Subscrição com formato inválido 
        if len(partes) < 2 or len(partes)>3 or "*" in partes[:-1]:
            return ERRO_TOPICO_INVALIDO
        with self.topicLock.read:
            region_node = self.topics.get(partes[0])

            if region_node is None:
                return ERRO_TOPICO_INEXISTENTE
    
            # Cancelar regiao/*
            if partes[1] == "*":
                node = region_node
    
            else:
                station_node = region_node.children.get(partes[1])
    
                if station_node is None:
                    return ERRO_TOPICO_INEXISTENTE
    
                # Cancelar regiao/estacao/* 
                if partes[2] == "*":
                    node = station_node
    
                else:
                    # Cancelar topico especifico 
                    node = station_node.children.get(partes[2])
    
                    if node is None:
                        return ERRO_TOPICO_INEXISTENTE
    
            if subscriber_id not in node.subscribers:
                return ERRO_SUBSCRICAO_INEXISTENTE

        #Retira Subscrição da Árvore de Subscrições
        with self.topicLock.write:
            node.subscribers.pop(subscriber_id)

        #Remove a subscricao do dicionario de portas do subscritor
        with self.subscritoresLock.write:
            self.subscritores[subscriber_id][1].pop(topico)

        return SUCESSO

    # ==================================================
    # TERMINACAO ORDENADA
    # ==================================================

    def terminacaoOrdenada(self, ps:bool, identificador:int)->int:
    
        # Caso seja um PUBLISHER
        if ps:
            with self.publishersLock.write:
                topicos = self.publishers.pop(identificador,[])

            with self.topicLock.write:
                for topico in topicos:
                    regiao, estacao, metrica = topico.split("/")
        
                    region_node = self.topics.get(regiao) 
                    if region_node is None:
                        continue
        
                    station_node = region_node.children.get(estacao)
                    if station_node is None:
                        continue
        
                    # Remover o tópico
                    station_node.children.pop(metrica, None) 
        
                    # Remover estação se ficar vazia
                    if not station_node.children and not station_node.subscribers:
                        region_node.children.pop(estacao, None)
        
                    # Remover região se ficar vazia
                    if not region_node.children and not region_node.subscribers:
                        self.topics.pop(regiao, None)

            with self.socketsLock.write:
                socket = self.sockets[0].pop(identificador,None)
                if socket is not None: socket.close()
    
        # Caso seja um SUBSCRIBER
        else:
            with self.subscritoresLock.write:
                tuplo = self.subscritores.get(identificador,None)
                topicos = list(tuplo[1].keys()) if tuplo is not None else []
    
            for topico in topicos:
                self.cancelarSubscricao(identificador,topico)

            with self.subscritoresLock.write:
                self.subscritores.pop(identificador,None)

            with self.socketsLock.write:
                socket = self.sockets[1].pop(identificador,None)
                if socket is not None: socket.close()
    
      
    
        return 0



    # ==================================================
    # PESQUISA DE DESTINATARIOS
    # ==================================================

    def obterDestinatarios(self, topico:str, message_id:int)->tuple[dict[int,tuple[str,int]] | None,  tuple[mylock.Lock ,dict[int, float]] | None]:
        partes = topico.split("/")
        
        if len(partes) != 3 or any(not parte or parte == "*" for parte in partes):
            return None, None

        regiao, estacao, metrica = partes

        with self.topicLock.read:
            # Procurar regiao
            region_node = self.topics.get(regiao)
    
            if region_node is None:
                return None, None
    
            # Procurar estacao
            station_node = region_node.children.get(estacao)
    
            if station_node is None:
                return None, None
    
            # Procurar topico
            metric_node = station_node.children.get(metrica)
    
            if metric_node is None:
                return None, None
    
            publication_qos = metric_node.qos
    
            nodes : list[list[tuple[int,int]]] = [
                list(region_node.subscribers.items()),
                list(station_node.subscribers.items()),
                list(metric_node.subscribers.items())
            ] 

        lista : list[tuple[tuple[int,tuple[str,int]],int]] = list()

        with self.subscritoresLock.read:
            filtro = f"{regiao}/*"
            for subscriber_id, subscription_qos in nodes[0]:
                porta = self.subscritores.get(subscriber_id,("",{}))[1].get(filtro,0)
                if porta != 0:
                    lista.append(((subscriber_id,(self.subscritores[subscriber_id][0],porta)),subscription_qos))
    
            filtro = f"{regiao}/{estacao}/*"
            for subscriber_id, subscription_qos in nodes[1]:
                porta = self.subscritores.get(subscriber_id,("",{}))[1].get(filtro,0)
                if porta != 0:
                    lista.append(((subscriber_id,(self.subscritores[subscriber_id][0],porta)),subscription_qos))
    
            filtro = f"{regiao}/{estacao}/{metrica}"
            for subscriber_id, subscription_qos in nodes[2]:
                porta = self.subscritores.get(subscriber_id,("",{}))[1].get(filtro,0)
                if porta != 0:
                    lista.append(((subscriber_id,(self.subscritores[subscriber_id][0],porta)),subscription_qos))

            

        
        with self.topicLock.write:
            metrica_registada = True if message_id in metric_node.messages else False
            # Caso o ack_dic não esteja vazio significa que estemos a lidar com um caso onde o ack do broker se perder, logo não temos que transmitir
            if metrica_registada and metric_node.messages[message_id]: return None,None
            ack_dic = metric_node.messages.get(message_id,(mylock.Lock(),{}))
            if metrica_registada is False:
                metric_node.messages[message_id]=ack_dic

        # Acumular subscritores encontrados, eliminar duplicados e definir QoS em uso
        matching_subscribers:dict[int,tuple[tuple[str,int],int]] = {}

        for sub,subscription_qos in lista:
            qos_atual = matching_subscribers.get(sub[0],((0,0),0))[1]
            if qos_atual == 0:
                matching_subscribers[sub[0]] = (sub[1],min(subscription_qos,publication_qos))
            elif publication_qos == 2 and qos_atual == 1 and subscription_qos == 2:
                matching_subscribers[sub[0]] = (sub[1],subscription_qos)

        resultado : dict[int,tuple[str,int]] = {k: v[0] for k, v in matching_subscribers.items() if v[1] == 1} if metrica_registada else {k: v[0] for k, v in matching_subscribers.items()}
        

        return resultado, ack_dic

    # =====================================================================
    # FUNCAO DE FECHO DE SOCKETS 
    # =====================================================================
    def fecha_sockets (self):
        with self.socketsLock.write:
            for s in self.sockets[0].values():
                s.shutdown(socket.SHUT_RDWR)
                s.close()
            for s in self.sockets[1].values():
                s.shutdown(socket.SHUT_RDWR)
                s.close()

  