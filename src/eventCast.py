
def interpretarEventCast(dados: bytes) -> tuple[bool, int, int, int, str, str]:
    posicao = 0
    # 1. FLAGS (8 bits)
    flags = dados[posicao]
    posicao += 1
    # ACK: bit 7
    ack = bool((flags >> 7) & 1)
    # QoS: bit 6
    # 0 = QoS 1
    # 1 = QoS 2
    qos = ((flags >> 6) & 1) + 1
    # 2. MESSAGE_ID (16 bits)
    message_id = int.from_bytes(dados[posicao:posicao + 2],byteorder="big")
    posicao += 2
    # 3. IDENTIFICADOR (8 bits)
    identificador = dados[posicao]
    posicao += 1
    comprimento = len(dados)
    if comprimento > 4:
        # 4. TAMANHO DO TÓPICO (8 bits)
        tamanho_topico = dados[posicao]
        posicao += 1
        # 5. TÓPICO
        topico = dados[posicao:posicao + tamanho_topico].decode("utf-8")
        posicao += tamanho_topico
        if comprimento > 5 + tamanho_topico:
            # 6. TAMANHO DA MÉTRICA (8 bits)
            tamanho_metrica = dados[posicao]
            posicao += 1
            # 7. MÉTRICA
            metrica = dados[posicao:posicao + tamanho_metrica].decode("utf-8")
        else:
            metrica = ""
    else:
        topico = ""
        metrica = ""
    return (
        ack,
        qos,
        message_id,
        identificador,
        topico,
        metrica
    )

def constroiMessage(ack: bool,qos: int,message_id: int,identificador: int,topico: str,metrica: str) -> bytes:

    # CONVERTER STRINGS PARA UTF-8
    topico_bytes = topico.encode("utf-8")
    metrica_bytes = metrica.encode("utf-8")
    # CONSTRUIR FLAGS
    # Bit 7 = ACK
    # Bit 6 = QoS (0 = QoS 1, 1 = QoS 2)
    # Bits 5-0 = Reservados (0)
    flags = (
        (int(ack) << 7) |
        ((qos-1)  << 6)
    )
    mensagem = bytearray()
    # 1. FLAGS (8 bits)
    mensagem.append(flags)
    # 2. MESSAGE_ID (16 bits)
    mensagem.extend(message_id.to_bytes(2, byteorder="big"))
    # 3. IDENTIFICADOR (8 bits)
    mensagem.append(identificador)
    if len(topico) > 0:
        # 4. TAMANHO DO TÓPICO (8 bits)
        mensagem.append(len(topico_bytes))
        # 5. TÓPICO
        mensagem.extend(topico_bytes)
        if len(metrica) > 0:
            # 6. TAMANHO DA MÉTRICA (8 bits)
            mensagem.append(len(metrica_bytes))
            # 7. MÉTRICA
            mensagem.extend(metrica_bytes)
    return bytes(mensagem)

def constroiAckMessage(message_id: int,identificador: int,topico:str) -> bytes:

    return constroiMessage(
        ack=True,
        qos=1,
        message_id=message_id,
        identificador=identificador,
        topico=topico,
        metrica=""
    )
