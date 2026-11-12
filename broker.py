
import socket
import threading
import time
import subCast
import eventCast

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
        self.subscribers = {}  # {subscriber_id: qos}
        self.children = {}     # {estacao: StationNode}


class StationNode:
    def __init__(self):
        # Subscritores de "regiao/estacao/*"
        self.subscribers = {}  # {subscriber_id: qos}
        self.children = {}     # {metrica: TopicLeaf}


class TopicLeaf:
    def __init__(self, qos):
        # QoS definida no registo do topico
        self.qos = qos
        self.messages = {}
        # Subscritores do topico especifico
        self.subscribers = {}  # {subscriber_id: qos}

# ==================================================
# BROKER
# ==================================================

class Broker:

    def __init__(self, broker_id, config):
        self.id = broker_id
        self.bind = config["bind"]
        self.control_port = config["control_port"]
        self.data_port = config["data_port"]
        self.duration = config["duration"]
        self.evic_after = config["evict_after"]
        # Dicionário onde vou manter os tópicos: Região -> Estação -> Métrica -> Dict(Subscribers,Qos)/Qos Publisher/Acks: Dicionario : Message_id -> (Lista de Waiting Acks)
        #Usar a presença de Message_id para verificar se a mensagem já foi enviada uma vez, isto é, se o broker já recebeu aquele message_id, ao procurar pelos subscritores a mandar, só recolhe os que tem QoS = 1 
        self.topics = {}
        # Dicionário de subscriber id -> Endereço
        self.subscritores = {}
        # Dicionário de publisher id -> Endereço
        self.publishers = {}

        self.running = True
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
    
            tcp_thread.join()
            udp_thread.join()

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

    def tratamentoSubControl(self, client_socket, client_address):
        try:
            client_socket.settimeout(self.evic_after*3)
            while True:
                buffer = subCast.recebeMensagemSubCast(client_socket)
                tamanhoTotal, es, ps, rs, cs, to, QoS, identificador, porta, topico = subCast.interpretar_SubCast(buffer)
                if (es): 
                    endereco = (client_address[0],porta)
                    identificador = self.atribuiIdentificador(ps,endereco)
                if (rs and topico):
                    if (ps): self.registoPub(topico,QoS)
                    else: qos = self.subscricaoSub(identificador,topico,QoS)
                if (cs): self.cancelarSubscricao(identificador,topico)
                if (to): self.terminacaoOrdenada(ps,identificador)
                self.confirmaSubControl(client_socket,identificador,qos)
                # Se foi gerada uma terminacao ordenada por parte do broker ao publisher ou ao subscriber ele deve terminar
                if (not ps and (not identificador in self.subscritores)) or (ps and (not identificador in self.publishers) or to):
                    break
        except OSError as e:
            print(e)
        finally: 
            client_socket.close()

    def confirmaSubControl(self, client_socket,identificador,qos):
        try:
            ack_message = subCast.constroiAckMessage(identificador,qos)
            client_socket.sendall(ack_message)
        except OSError as e:
            raise e

    def tratamentoEventCast(self, mensagem, addr):
        ack, qos, message_id, identificador, topico, metrica = eventCast.interpretar_EventCast(mensagem)
        # Estamos a receber um ACK de um subscritor tem de ter o message_id, identificador e topico, neste caso, metrica = null
        # Preferivel: Ler o endereço de onde veio a mensagem e map é feito com base no endereço do subscritor: Menos dados enviado pela rede mas torna o campo identificador inutil, dado que podemos sempre identificar o subscritor pelo par (IP,Porta) origem do pacote dado pelo addr 
        if (ack):
            pass
        # Estamos a receber uma nova métrica de um publisher. Neste caso o identificador é o do publisher que acaba por ser também inutil
        else:
            self.enviaACKEventCast(message_id,addr)
            #Operação com Lock para ver quem são os destinatários a enviar!
            subscritores, ack_dic = self.obterDestinatarios(topico,message_id)
            # Estou a ignorar o caso onde temos uma race entre a chegada de um unsubscribe e o envio de um tópico, caso contrário eu ia precisar de ter um lock bloqueado durante o tempo em que estou a tentar enviar uma mensagem para um subscriber, a melhor opção seria ter um lock por subscriber, cc teria tudo bloqueado durante muito tempo.
            for id_sub in subscritores:
                #Problemas com lock aqui tambem -> Estrutura de endereços partilhada por todas as threads
                if id_sub in self.subscritores: # Muito ineficiente mas ao menos ajuda a filtrar casos onde ele seja terminado
                    endereco = self.subscritores[id_sub]["endereco"]
                    self.enviaPublicacao(endereco,mensagem)
                    ack_dic[id_sub] = time.monotonic()
            while not ack_dic:
                time.sleep(1)
                #Estamos a ignorar problemas de concorrencia entre receber um ACK e estar a reenviar, precisaria de um lock
                for id_sub, tempoEspera in ack_dic.items():
                    if id_sub in self.subscritores: # Muito ineficiente mas ao menos ajuda a filtrar casos onde ele seja terminado
                        endereco = self.subscritores[id_sub]["endereco"]
                        if time.monotonic() - tempoEspera > self.evic_after:
                            # O tempo máximo de espera por uma resposta acabou -> Aciona a terminação do sub geral e remove-o da lista de espera
                            # Problema caso estejamos a enviar mais do que uma métrica para este sub ele será removido completamente, contudo não é removido da lista de acks de cada topico
                            thread = threading.Thread(target=self.terminacaoOrdenada,args=(False, id_sub))
                            thread.start()
                            del ack_dic[id_sub]
                        else:
                            self.enviaPublicacao(endereco,mensagem)
                    else: del ack_dic[id_sub] #Quando ele é terminado a meio deve ser removido da lista de acks em espera
        #Quando o ack_dic terminar signfica que já recebemos a confirmação de todos os subs ativos, ou seja, já terminados o processo de envio
        return
    
        #Em caso de publicação verifica quantos fragmentos tem a publicação e se >1 cria uma thread que vai armazenar qual o fragmento recebido e enviar um ACK para a origem do número de segmentos continuos mais alto ja recebido. Depois verifica se já os tem todos, em caso afirmativo segue para a função de envio para o destino 
        #Em caso de ACK regista na estrutura auxiliar de receção dos ACKS e elimina todos os fragmentos que ja foram confirmados até ao momento da estrutura (se já forem todos remove tambem o registo daquela mensagem da estrutura)
        


    #PODIA FAZER UM MELHOR TRATAMENTO DE ERROS
    def enviaPublicacao(self,endereco,mensagem):
        try:
            self.data_socket.sendto(mensagem,endereco)
        except OSError as e:
            return

    def enviaACKEventCast(self,message_id, addr):
        ack_message = eventCast.constroiAckMessage(message_id,0)
        try:
            self.data_socket.sendto(ack_message,addr)
        except OSError as e:
            return

    # ==================================================
    # ESTABELECIMENTO DE SESSÃO (ATRIBUIÇÃO DO IDENTIFICADOR)
    # ==================================================

    def atribuiIdentificador(self,ps, ip, porta): 
        dicionario = self.publishers if ps else self.subscritores
        identificador = len(dicionario)+1
        if ps: dicionario[identificador]=[]
        else: dicionario[identificador]= {
            "endereco": (ip,porta),
            "topicos": []
            }
        return identificador
                

    # ==================================================
    # REGISTO DE PUBLICACAO / TOPICO
    # ==================================================

    def registoPub(self,publisher_id, topico, qos):
        """
        Regista um topico na arvore.

        Recebe:
            topico: regiao/estacao/metrica
            qos: QoS do topico (1 ou 2)

        Retorna:
            SUCESSO
            TOPICO_JA_EXISTE
            ERRO_TOPICO_INVALIDO
        """

        partes = topico.split("/")

        if len(partes) != 3 or any(not parte or parte == "*" for parte in partes):
            return ERRO_TOPICO_INVALIDO

        regiao, estacao, metrica = partes

        region_node = self.topics.get[regiao]

        # Criar regiao, caso nao exista
        if region_node is None:
            region_node = RegionNode()
            self.topics[regiao] = region_node

        station_node = self.topics.get[estacao]

        # Criar estacao, caso nao exista
        if estacao is None:
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

    def subscricaoSub(self, subscriber_id, topico, qos):
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
        self.subscritores[subscriber_id]["topicos"].append(topico)
    
        return 0

    # ==================================================
    # CANCELAMENTO DE SUBSCRICAO
    # ==================================================
    def cancelarSubscricao(self, subscriber_id, topico):
        """
        Cancela uma subscricao especifica.
        """

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

        #Reduz o número de subscrições ativas na tabela de endereços e portas
        self.subscritores[subscriber_id]["topicos"].remove(topico)

        return SUCESSO

    # ==================================================
    # TERMINACAO ORDENADA
    # ==================================================

    def terminacaoOrdenada(self, ps, identificador):
    
        dicionario = self.publishers if ps else self.subscritores
    
        if identificador not in dicionario:
            return -1
    
        # Caso seja um PUBLISHER
        if ps:
            topicos = dicionario[identificador]
    
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
    
        # Caso seja um SUBSCRIBER
        else:
            topicos = dicionario[identificador]["topicos"]
    
            for topico in topicos:
                self.cancelarSubscricao(identificador,topico)
    
        # Remover publisher ou subscriber do respetivo dicionário
        del dicionario[identificador]
    
        return 0



    # ==================================================
    # PESQUISA DE DESTINATARIOS
    # ==================================================

    def obterDestinatarios(self, topico, message_id):
        """
        Procura os subscritores interessados num topico.

        A QoS da publicacao e obtida da folha.

        Retorna:
            {subscriber_id: qos_efetiva}

        Cada subscritor aparece apenas uma vez.
        """

        partes = topico.split("/")
        
        if len(partes) != 3 or any(not parte or parte == "*" for parte in partes):
            return ERRO_TOPICO_INVALIDO

        regiao, estacao, metrica = partes

        # Procurar regiao
        region_node = self.topics.get(regiao)

        if region_node is None:
            return None

        # Procurar estacao
        station_node = region_node.children.get(estacao)

        if station_node is None:
            return None

        # Procurar topico
        metric_node = station_node.children.get(metrica)

        if metric_node is None:
            return None

        publication_qos = metric_node.qos

        # Acumular subscritores encontrados, eliminar duplicados e definir QoS em uso
        matching_subscribers = {}

        nodes = [
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

  