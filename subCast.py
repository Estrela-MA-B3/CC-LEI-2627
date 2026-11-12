import util 

SUBCAST_HEADER_SIZE = 4


def interpretar_SubCast(dados):

    # Verificar se existem pelo menos 2 bytes
    if len(dados) < 2:
        raise ValueError("Cabeçalho incompleto")

    # --------------------------------
    # BYTE 1 - FLAGS DE CONTROLO
    # --------------------------------

    flags = dados[0]

    estabelecer_sessao = (flags >> 7) & 1
    publisher_subscriber = (flags >> 6) & 1
    registo_subscricao = (flags >> 5) & 1
    cancelamento_subscricao = (flags >> 4) & 1
    terminacao = (flags >> 3) & 1
    qos = (flags >> 2) & 1

    # --------------------------------
    # BYTE 2 - IDENTIFICAÇÃO
    # --------------------------------

    identificacao = dados[1]

    # Construir resultado inicial
    resultado = {
        "estabelecer_sessao": estabelecer_sessao,
        "publisher_subscriber": publisher_subscriber,
        "registo_subscricao": registo_subscricao,
        "cancelamento_subscricao": cancelamento_subscricao,
        "terminacao": terminacao,
        "qos": qos,
        "identificacao": identificacao
    }

    # --------------------------------
    # CAMPOS OPCIONAIS
    # Apenas se for registo/subscrição
    # --------------------------------

    if registo_subscricao == 1:

        # Verificar se existe o terceiro byte
        if len(dados) < 3:
            raise ValueError("Campo opcional incompleto")

        campo = dados[2]

        # Primeiro bit: primeira ou última opção
        opcao = (campo >> 7) & 1

        # Últimos 7 bits: tamanho do tópico
        tamanho = campo & 0b01111111

        # --------------------------------
        # INTERPRETAÇÃO DO TÓPICO
        # --------------------------------

        topico_bytes = dados[3:3 + tamanho]

        if len(topico_bytes) != tamanho:
            raise ValueError("Tópico incompleto")

        topico = util.interpretar_String(topico_bytes)

        resultado["opcao"] = opcao
        resultado["tamanho_topico"] = tamanho
        resultado["topico"] = topico

    return resultado

def interpretar_opcoes(dados, offset=2):

    opcoes = []

    while offset < len(dados):

        # Ler os 2 bytes do campo opcional
        campo = (dados[offset] << 8) | dados[offset + 1]

        # Primeiro bit: existem mais tópicos?
        mais_topicos = (campo >> 15) & 1

        # Últimos 15 bits: comprimento
        tamanho = campo & 0b0111111111111111

        offset += 2

        # Extrair tópico
        topico_bytes = dados[offset:offset + tamanho]

        if len(topico_bytes) != tamanho:
            raise ValueError("Tópico incompleto")

        topico = interpretar_caracteres(topico_bytes)

        offset += tamanho

        opcoes.append({
            "mais_topicos": mais_topicos,
            "tamanho_topico": tamanho,
            "topico": topico
        })

        if mais_topicos == 0:
            break

    return opcoes

def constroiAckMessage(identificador,qos):
    return

def controiMessage(es,ps,rs,cs,to,qos,close,id,topico,porta):
    return


def receberBytes(client_socket, quantidade):
    buffer = bytearray()

    while len(buffer) < quantidade:
        dados = client_socket.recv(quantidade - len(buffer))

        if not dados:
            raise ConnectionError("Ligação TCP terminada")

        buffer.extend(dados)

    return buffer


def recebeMensagemSubCast(client_socket):

    # Receber os 2 bytes que indicam o tamanho total
    mensagem = receberBytes(client_socket, 2)

    tamanhoTotal = util.traduzInt(mensagem, 16)

    if tamanhoTotal < SUBCAST_HEADER_SIZE:
        raise ValueError("Tamanho de mensagem inválido")

    # Receber os restantes bytes
    restantes = receberBytes(client_socket, tamanhoTotal - 2)

    mensagem.extend(restantes)

    return mensagem

