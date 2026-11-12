import time 

def traduzInt (mensagem,n):
        if n == 0:
            return 0
        if n < 0 or n > len(mensagem) * 8:
            raise ValueError("O número de bits 'n' é inválido para o tamanho do array.")
        # Calcula quantos bytes completos (ou parciais) são necessários
        num_bytes = (n + 7) // 8
        # Converte apenas os bytes relevantes para inteiro
        valor = int.from_bytes(mensagem[:num_bytes], byteorder='big')
        # Remove os bits que sobram à direita do último byte lido
        bits_sobra = (num_bytes * 8) - n
        return valor >> bits_sobra

def interpretar_String(dados, n):
    return dados[:n].decode("utf-8")

def filtroCorresponde(filtro, topico):
    if filtro.endswith("/*"):
        return topico.startswith(filtro[:-1])
    return filtro == topico

def registaLog(file_path, identificador, topico, message_id):
            timestamp_ms = time.time_ns() // 1_000_000
        
            with open(file_path, "a", encoding="utf-8") as ficheiro:
                ficheiro.write(f"{timestamp_ms} {identificador} {topico} {message_id}\n")
                ficheiro.flush()