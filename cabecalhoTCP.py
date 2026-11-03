
def interpretar_caracteres(dados):
#NÃO É PARA MANTER ASSIM. NÃO SE PODE USAR DECODE!
    return dados.decode("utf-8")


def interpretar_cabecalho(dados):

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

        topico = interpretar_caracteres(topico_bytes)

        resultado["opcao"] = opcao
        resultado["tamanho_topico"] = tamanho
        resultado["topico"] = topico

    return resultado

