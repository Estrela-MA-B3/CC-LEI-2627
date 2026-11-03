
class LeitorBits:

    def __init__(self, dados):
        self.dados = dados
        self.posicao = 0
        self.limite = len(dados) * 8

    def ler_bits(self, quantidade):

        if self.posicao + quantidade > self.limite:
            raise ValueError("Dados insuficientes")

        valor = 0

        for _ in range(quantidade):

            byte_atual = self.dados[self.posicao // 8]

            pos_bit = 7 - (self.posicao % 8)

            bit = (byte_atual >> pos_bit) & 1

            valor = (valor << 1) | bit

            self.posicao += 1

        return valor

    def ler_bytes(self, quantidade):

        resultado = bytearray()

        for _ in range(quantidade):
            resultado.append(self.ler_bits(8))

        return bytes(resultado)


def interpretar_caracteres(dados):
    """
    Interpretação provisória do tópico.
    """
    return dados.decode("utf-8")


def interpretar_metrica(leitor):
    """
    Função auxiliar para interpretar o valor da métrica.

    A implementação será definida quando for conhecido
    o tipo de dados utilizado.

    A função deverá ler os bits correspondentes à métrica
    e devolver o respetivo valor.
    """

    raise NotImplementedError(
        "Tipo de dados da métrica ainda não definido"
    )


def interpretar_cabecalho_aplicacional(dados):

    leitor = LeitorBits(dados)

    # --------------------------------
    # MESSAGE ID
    # --------------------------------

    message_id = leitor.ler_bits(MESSAGE_ID_BITS)

    # --------------------------------
    # ACK
    # --------------------------------

    ack = leitor.ler_bits(ACK_BITS)

    # --------------------------------
    # NUMERO DE SEQUENCIA
    # --------------------------------

    sequence_number = leitor.ler_bits(SEQUENCE_BITS)

    # --------------------------------
    # LENGTH
    # --------------------------------

    length = leitor.ler_bits(LENGTH_BITS)

    # O length representa bytes da secção
    # de opções que vem a seguir.

    fim_opcoes = leitor.posicao + (length * 8)

    if fim_opcoes > len(dados) * 8:
        raise ValueError("Dados aplicacionais incompletos")

    # Limitar a leitura à secção de opções
    leitor.limite = fim_opcoes

    resultado = {
        "message_id": message_id,
        "ack": ack,
        "sequence_number": sequence_number,
        "length": length,
        "opcoes": []
    }

    # --------------------------------
    # INTERPRETAR OPCOES
    # --------------------------------

    while leitor.posicao < fim_opcoes:

        # Bit que indica se existe outra opção
        mais_opcoes = leitor.ler_bits(1)

        # Comprimento do tópico (7 bits)
        tamanho_topico = leitor.ler_bits(TOPIC_LENGTH_BITS)

        # Ler bytes do tópico
        topico_bytes = leitor.ler_bytes(tamanho_topico)

        topico = interpretar_caracteres(topico_bytes)

        # --------------------------------
        # VALOR DA METRICA
        # --------------------------------

        metrica = interpretar_metrica(leitor)

        opcao = {
            "mais_opcoes": mais_opcoes,
            "tamanho_topico": tamanho_topico,
            "topico": topico,
            "metrica": metrica
        }

        resultado["opcoes"].append(opcao)

        # Se não existirem mais opções, terminar
        if mais_opcoes == 0:
            break

    # Verificar se a secção foi interpretada
    # sem ultrapassar o comprimento declarado

    if leitor.posicao != fim_opcoes:
        raise ValueError(
            "Comprimento das opções inconsistente"
        )

    return resultado
