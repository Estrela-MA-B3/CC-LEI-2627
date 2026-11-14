from typing import TypedDict, TypeAlias

# Tipos auxiliares
EnderecoPorta: TypeAlias = tuple[str, int]
FluxoPublicacao: TypeAlias = tuple[str, int, int, int]
Subscricao: TypeAlias = tuple[str, int]
AcaoSubscricao: TypeAlias = tuple[int, bool, str, int]

# Configuração do Broker
class ConfigBroker(TypedDict):
    id: int
    bind: str
    control_port: int
    data_port: int
    duration: int
    evict_after: int
    peer: list[EnderecoPorta]

# Configuração do Publisher
class ConfigPublisher(TypedDict):
    id: int
    broker_control: EnderecoPorta
    log: str
    start_after: int
    drain: int
    publish: list[FluxoPublicacao]

# Configuração do Subscriber
class ConfigSubscriber(TypedDict):
    id: int
    broker_control: EnderecoPorta
    log: str
    duration: int
    subscribe: list[Subscricao]
    at: list[AcaoSubscricao]

def _ler_linhas(ficheiro: str) -> list[tuple[int, str]]:
    """Lê o ficheiro, removendo comentários e linhas vazias."""
    linhas: list[tuple[int, str]] = []

    with open(ficheiro, "r", encoding="utf-8") as f:
        for numero, linha in enumerate(f, start=1):
            linha = linha.split("#", 1)[0].strip()

            if linha:
                linhas.append((numero, linha))

    return linhas

def _endereco_porta(valor: str) -> EnderecoPorta:
    endereco, porta = valor.split(":")
    return endereco, int(porta)

def ler_config_broker(
    ficheiro: str,
    identificador: int
) -> ConfigBroker:

    config: ConfigBroker = {
        "id": identificador,
        "bind": "",
        "control_port": 0,
        "data_port": 0,
        "duration": 0,
        "evict_after": 0,
        "peer": []
    }

    for _, linha in _ler_linhas(ficheiro):
        chave, valor = map(str.strip, linha.split("=", 1))

        if chave == "bind":
            config["bind"] = valor

        elif chave in ("control_port", "data_port",
                       "duration", "evict_after"):
            config[chave] = int(valor)

        elif chave == "peer":
            config["peer"].append(_endereco_porta(valor))

    return config

def ler_config_publisher(
    ficheiro: str,
    identificador: int
) -> ConfigPublisher:

    config: ConfigPublisher = {
        "id": identificador,
        "broker_control": ("", 0),
        "log": "",
        "start_after": 0,
        "drain": 0,
        "publish": []
    }

    for _, linha in _ler_linhas(ficheiro):
        chave, valor = map(str.strip, linha.split("=", 1))

        if chave == "broker_control":
            config["broker_control"] = _endereco_porta(valor)

        elif chave == "log":
            config["log"] = valor

        elif chave in ("start_after", "drain"):
            config[chave] = int(valor)

        elif chave == "publish":
            partes = valor.split()

            topico = partes[0]
            qos = int(partes[1].split("=")[1])
            count = int(partes[2].split("=")[1])
            rate = int(partes[3].split("=")[1])

            config["publish"].append(
                (topico, qos, count, rate)
            )

    return config

def ler_config_subscriber(
    ficheiro: str,
    identificador: int
) -> ConfigSubscriber:

    config: ConfigSubscriber = {
        "id": identificador,
        "broker_control": ("", 0),
        "log": "",
        "duration": 0,
        "subscribe": [],
        "at": []
    }

    for _, linha in _ler_linhas(ficheiro):
        chave, valor = map(str.strip, linha.split("=", 1))

        if chave == "broker_control":
            config["broker_control"] = _endereco_porta(valor)

        elif chave == "log":
            config["log"] = valor

        elif chave == "duration":
            config["duration"] = int(valor)

        elif chave == "subscribe":
            partes = valor.split()

            filtro = partes[0]
            qos = int(partes[1].split("=")[1])

            config["subscribe"].append((filtro, qos))

        elif chave == "at":
            partes = valor.split()
            tempo = int(partes[0])

            if partes[1].startswith("subscribe="):
                filtro = partes[1].split("=", 1)[1]
                qos = int(partes[2].split("=")[1])

                config["at"].append(
                    (tempo, True, filtro, qos)
                )

            elif partes[1].startswith("unsubscribe="):
                filtro = partes[1].split("=", 1)[1]

                config["at"].append(
                    (tempo, False, filtro, 0)
                )

    return config
