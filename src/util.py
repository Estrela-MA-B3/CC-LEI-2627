import time 

def interpretar_String(dados:bytearray, n:int)->str:
    return dados[:n].decode("utf-8")

def filtroCorresponde(filtro:str, topico:str)->bool:
    if filtro.endswith("/*"):
        return topico.startswith(filtro[:-1])
    return filtro == topico

def registaLog(file_path:str, identificador:int, topico:str, message_id:int):
            timestamp_ms = time.time_ns() // 1_000_000
        
            with open(file_path, "a", encoding="utf-8") as ficheiro:
                ficheiro.write(f"{timestamp_ms} {identificador} {topico} {message_id}\n")
                ficheiro.flush()