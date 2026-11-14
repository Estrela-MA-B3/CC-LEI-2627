
import socket
import threading
import time
import subControl
import eventCast
import configParser

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
        self.messages:dict[int, dict[int, tuple[float, tuple[str, int]]]] = {}
        # Subscritores do topico especifico
        self.subscribers:dict[int,int] = {}  # {subscriber_id: qos}

# ==================================================
# BROKER
# ==================================================

class Broker:

    def __init__(self, broker_id:int, config:configParser.ConfigBroker):
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
        self.publishers:dict[int,list[str]] = {}

        self.running:bool = True
        self.erro_tcp = None
        self.erro_udp = None

        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)


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
    
            if tcp_thread is not None: tcp_thread.join()
            if udp_thread is not None: udp_thread.join()

    # ==================================================
    # FUNÇÃO DE CICLO DOS SOCKETS
    # ==================================================
    
    
    def cicloTCP(self):
        while self.running:
            try:
                client_socket, client_address = self.control_socket.accept()
    
                thread = threading.Thread(target=self.tratamentoSubControl,args=(client_socket, client_address))
                thread.start()

            #Termina com o close e a excessão associada caso contrário colocamos o timeout e esta condição para ele avaliar o running periodicamente
            #except socket.timeout:
            #    continue
    
            except OSError as e:
                if self.running:
                    self.erro_tcp = e
                break    
    
    def cicloUDP(self):
        while self.running:
            try:
                mensagem, addr = self.data_socket.recvfrom(1024)
    
                thread = threading.Thread(target=self.tratamentoEventCast,args=(mensagem, addr))
                thread.start()
    
            except OSError as e:
                if self.running:
                    self.erro_udp = e
                break

    # ==================================================
    # TRATAMENTO DE UMA MENSAGEM RECEBIDA
    # ==================================================

    def tratamentoSubControl(self, client_socket:socket.socket, client_address:tuple[str,int]):
        try:
            while True:
                buffer = subControl.recebeMensagemSubControl(client_socket)
                tamanhoTotal, udp, ps, rs, cs, to, QoS, identificador, porta, topico = subControl.interpretar_SubControl(buffer) 
                if (rs and topico):
                    if (ps): self.registoPub(identificador,topico,QoS)
                    else: self.subscricaoSub(identificador,topico,QoS,porta,client_address[0])
                if (cs): self.cancelarSubscricao(identificador,topico)
                if (to): self.terminacaoOrdenada(ps,identificador)
                # So enviamos uma mensagem se estiver a pedir a porta UDP ou se for uma subscricao para comunicar o qos
                if (udp): self.mensagemSubControl(client_socket,self.id)
                # Se foi gerada uma terminacao ordenada por parte do broker ao publisher ou ao subscriber ele deve terminar
                if (not ps and (not identificador in self.subscritores)) or (ps and (not identificador in self.publishers) or to):
                    break
        except OSError as e:
            pass
        finally: 
            client_socket.close()

    def mensagemSubControl(self, client_socket:socket.socket,identificador:int):
        try:
            ack_message = subControl.constroiAckMessage(identificador,self.data_port)
            client_socket.sendall(ack_message)
        except OSError as e:
            raise e

    def tratamentoEventCast(self, mensagem:bytes, addr:tuple[str,int]):
        ack, qos, message_id, identificador, topico, metrica = eventCast.interpretarEventCast(mensagem)
        # Estamos a receber um ACK de um subscritor tem de ter o message_id, identificador e topico, neste caso, metrica = null
        # Preferivel: Ler o endereço de onde veio a mensagem e map é feito com base no endereço do subscritor: Menos dados enviado pela rede mas torna o campo identificador inutil, dado que podemos sempre identificar o subscritor pelo par (IP,Porta) origem do pacote dado pelo addr 
        if (ack):
            partes = topico.split("/")
            if len(partes) != 3 or any(not parte or parte == "*" for parte in partes):return
            if partes[0] in self.topics:
                if partes[1] in self.topics[partes[0]].children:
                    if partes[2] in self.topics[partes[0]].children[partes[1]].children:
                        if message_id in self.topics[partes[0]].children[partes[1]].children[partes[2]].messages:
                            ack_dic = self.topics[partes[0]].children[partes[1]].children[partes[2]].messages[message_id]
                            if identificador in ack_dic: del ack_dic[identificador]
        # Estamos a receber uma nova métrica de um publisher. Neste caso o identificador é o do publisher que acaba por ser também inutil
        else:
            thread_ack = threading.Thread(target=self.enviaACKEventCast,args=(message_id,addr))
            thread_ack.start()
            #Operação com Lock para ver quem são os destinatários a enviar!
            subscritores, ack_dic = self.obterDestinatarios(topico,message_id)
            if subscritores is None or ack_dic is None: return
            # Colocar Lock na estrtutura para não dar erros de escrita vs leitura
            for id_sub in subscritores:
                #Problemas com lock aqui tambem -> Estrutura de endereços partilhada por todas as threads
                sub = self.subscritores.get(id_sub)
                if sub is not None: 
                    endIP = sub[0]
                    porta = sub[1].get(topico)
                    if porta is not None: 
                        self.enviaPublicacao((endIP,porta),mensagem)
                        ack_dic[id_sub] = (time.monotonic(),(endIP,porta))
            while not ack_dic:
                time.sleep(1)
                #Estamos a ignorar problemas de concorrencia entre receber um ACK e estar a reenviar, precisaria de um lock -> Devemos adquirir lock para copiar quem vamos enviar e depois enviar, mas o lock deve ser o mais curto possivel para não bloquear a receção de ACKS
                for id_sub, tuplo in ack_dic.items():
                        if time.monotonic() - tuplo[0] > self.evic_after:
                            thread = threading.Thread(target=self.terminacaoOrdenada,args=(False, id_sub))
                            thread.start()
                            del ack_dic[id_sub]
                        #Verifica se ele foi terminado ou se fez unsubscribe
                        if id_sub not in self.subscritores or self.subscritores[id_sub][1].get(topico) is None: del ack_dic[id_sub]
                        else: self.enviaPublicacao(tuplo[1],mensagem)
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

        region_node = self.topics.get(regiao)

        # Criar regiao, caso nao exista
        if region_node is None:
            region_node = RegionNode()
            self.topics[regiao] = region_node

        station_node = region_node.children.get(estacao)

        # Criar estacao, caso nao exista
        if station_node is None:
            station_node = StationNode()
            region_node.children[estacao] = station_node

        # Verificar se o topico ja existe
        if metrica in station_node.children:
            return TOPICO_JA_EXISTE

        # Criar folha com a QoS
        station_node.children[metrica] = TopicLeaf(qos)

        self.publishers[publisher_id].append(topico)

        return SUCESSO

    # ==================================================
    # SUBSCRICAO
    # ==================================================

    def subscricaoSub(self, subscriber_id:int, topico:str, qos:int, portaUDP:int, endIP:str)->int:
        partes = topico.split("/")

        # Subscrição com formato inválido 
        if len(partes) < 2 or len(partes)>3:
            return -1

        region_node = self.topics.get(partes[0])

        # Nodo da Região não existe
        if region_node is None:
                        return -1
    
        # Subscrição de uma região inteira: north/*
        elif len(partes) == 2 and partes[1] == "*":
            node = region_node
    
        # Subscrição de uma estação inteira ou tópico específico
        elif len(partes) == 3:
            station_node = region_node.children.get(partes[1])

            if station_node is None:
                return -1
    
            if partes[2] == "*":
                node = station_node
    
            else:
                metric_node = station_node.children.get(partes[2])
                if metric_node is None:
                    return -1
                node = metric_node
    
        else:
            return -1

        #Regista a subscrição no dicionário onde coloca o id do subscritor como chave e qos como valor
        node.subscribers[subscriber_id] = qos
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

            # Cancelar regiao/estacao/* -> PROBLEMA SE ELE ESTIVER NUMA DAS FOLHAS DE ACKS ELE NAO É REMOVIDO
            if partes[2] == "*":
                node = station_node

            else:
                # Cancelar topico especifico -> PRECISA DE O REMOVER DA ESTRUTURA DE ACKS DA MESSAGE_ID
                node = station_node.children.get(partes[2])

                if node is None:
                    return ERRO_TOPICO_INEXISTENTE

        if subscriber_id not in node.subscribers:
            return ERRO_SUBSCRICAO_INEXISTENTE

        #Retira Subscrição da Árvore de Subscrições
        del node.subscribers[subscriber_id]

        #Remove a subscricao do dicionario de portas do subscritor
        self.subscritores[subscriber_id][1].pop(topico, None)

        return SUCESSO

    # ==================================================
    # TERMINACAO ORDENADA
    # ==================================================

    def terminacaoOrdenada(self, ps:bool, identificador:int)->int:
    
        dicionario = self.publishers if ps else self.subscritores
    
        if identificador not in dicionario:
            return -1
    
        # Caso seja um PUBLISHER
        if ps:
            topicos = dicionario[identificador]
    
            for topico in topicos:
                regiao, estacao, metrica = topico.split("/") # type: ignore
    
                region_node = self.topics.get(regiao) # type: ignore
                if region_node is None:
                    continue
    
                station_node = region_node.children.get(estacao) # type: ignore
                if station_node is None:
                    continue
    
                # Remover o tópico
                station_node.children.pop(metrica, None) # type: ignore
    
                # Remover estação se ficar vazia
                if not station_node.children and not station_node.subscribers:
                    region_node.children.pop(estacao, None) # type: ignore
    
                # Remover região se ficar vazia
                if not region_node.children and not region_node.subscribers:
                    self.topics.pop(regiao, None) # type: ignore
    
        # Caso seja um SUBSCRIBER
        else:
            topicos = dicionario[identificador][1]
    
            for topico in list(topicos):
                self.cancelarSubscricao(identificador,topico)
    
        # Remover publisher ou subscriber do respetivo dicionário
        del dicionario[identificador]
    
        return 0



    # ==================================================
    # PESQUISA DE DESTINATARIOS
    # ==================================================

    def obterDestinatarios(self, topico:str, message_id:int)->tuple[list[int] | None,  dict[int, tuple[float, tuple[str, int]]] | None]:
        partes = topico.split("/")
        
        if len(partes) != 3 or any(not parte or parte == "*" for parte in partes):
            return None, None

        regiao, estacao, metrica = partes

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

        # Acumular subscritores encontrados, eliminar duplicados e definir QoS em uso
        matching_subscribers:dict[int,int] = {}

        nodes : list[RegionNode|StationNode|TopicLeaf] = [
            region_node,
            station_node,
            metric_node
        ]   

        for node in nodes:
            for subscriber_id, subscription_qos in node.subscribers.items():
                matching_subscribers[subscriber_id] = min (publication_qos, max(
                    matching_subscribers.get(subscriber_id, subscription_qos),
                    subscription_qos
                ))
        if message_id in metric_node.messages:
            matching_subscribers = {k: v for k, v in matching_subscribers.items() if v == 2}
        else: metric_node.messages[message_id]={}
        return list(matching_subscribers), metric_node.messages[message_id]

  