import socket

SUBCONTROL_HEADER_SIZE = 3

def interpretar_SubControl(dados: bytearray) -> tuple[int, bool, bool, bool, bool, bool, int, int, int, str]:

    # Ler os campos obrigatórios
    tamanhoTotal = dados[0]
    flags = dados[1]
    identificador = dados[2]

    # Interpretar as flags
    udp = bool(flags & 0b10000000)
    ps  = bool(flags & 0b01000000)
    rs  = bool(flags & 0b00100000)
    cs  = bool(flags & 0b00010000)
    to  = bool(flags & 0b00001000)
    qos = ((flags >> 2) & 0b00000001 ) + 1
    # Bits 1 e 0 são reservados para flags futuras

    posicao = 3

    porta = 0
    topico = ""

    # A porta UDP está presente se rs=True e ps=False
    if rs and not ps:

        if posicao + 2 > tamanhoTotal:
            raise ValueError("Mensagem incompleta: falta a porta UDP.")

        porta = int.from_bytes(
            dados[posicao:posicao + 2],
            byteorder="big"
        )

        posicao += 2

    # O tópico está presente se rs=True ou cs=True
    if rs or cs:
            topico = dados[posicao:tamanhoTotal].decode("utf-8")

    return (
        tamanhoTotal,
        udp,
        ps,
        rs,
        cs,
        to,
        qos,
        identificador,
        porta,
        topico
    )

def constroiAckMessage(identificador: int, portaUDP: int) -> bytes:
    return controiMessage(
        udp=False,
        ps=False,
        rs=True,
        cs=False,
        to=False,
        qos=1,
        id=identificador,
        topico="",
        porta=portaUDP
    )

def controiMessage(udp: bool,ps: bool,rs: bool,cs: bool,to: bool,qos: int,id: int,topico: str,porta: int) -> bytes:
   
    # Construir o byte de flags
    flags = (
        (int(udp) << 7) |
        (int(ps)  << 6) |
        (int(rs)  << 5) |
        (int(cs)  << 4) |
        (int(to)  << 3) |
        ((qos-1)  << 2)
    )

    # Os bits 1 e 0 ficam reservados (0)

    # Criar mensagem
    mensagem = bytearray()

    # Byte 0: espaço reservado para TamanhoTotal
    mensagem.append(0)

    # Byte 1: Flags
    mensagem.append(flags)

    # Byte 2: Identificador
    mensagem.append(id)

    # Incluir porta UDP se rs=True e ps=False
    if rs and not ps:
        mensagem.extend(porta.to_bytes(2, byteorder="big"))

    # Incluir tópico se rs=True ou cs=True
    if rs or cs:
        mensagem.extend(topico.encode("utf-8"))

    # Calcular TamanhoTotal (cabeçalho + payload)
    tamanhoTotal = len(mensagem)

    # Atualizar primeiro byte com o tamanho total
    mensagem[0] = tamanhoTotal

    return bytes(mensagem)

def recebeMensagemSubControl(client_socket: socket.socket) -> bytearray:
    resultado = bytearray()
    try:
        tamanhoTotalB = client_socket.recv(1)
        if not tamanhoTotalB:
            raise ConnectionError("Ligação TCP terminada")
        tamanhoTotal = tamanhoTotalB[0]
        if tamanhoTotal < SUBCONTROL_HEADER_SIZE:
            raise ValueError("Tamanho de mensagem inválido")
        resultado.extend(tamanhoTotalB)
        em_falta = tamanhoTotal - 1
        while em_falta > 0:
            dados = client_socket.recv(em_falta)
            if not dados:
                raise ConnectionError("Ligação terminada a meio de uma mensagem")
            resultado.extend(dados)
            em_falta -= len(dados)
    except OSError:
        raise
    return resultado

