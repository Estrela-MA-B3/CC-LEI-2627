#!/usr/bin/env python3
"""Gera as configurações (secção 5.2 do enunciado) de cada cenário de teste.

Cada cenário é uma pasta em cenarios/<nome>/ com um ficheiro <no>.conf por nó
do CORE que participa (broker1, pub1, sub3, ...). O lancar.sh usa o nome do
ficheiro para saber em que nó do CORE corre e com que papel.

Os registos vão para /tmp/tp2/<cenario>/<no>.log (caminho absoluto, como pede
o contrato). O lancar.sh cria essa pasta antes de arrancar.
"""
import os
import shutil

CONTROL_PORT = 7310
DATA_PORT = 7311
IP = {"broker1": "10.0.3.20", "broker2": "10.0.4.20", "broker3": "10.0.5.20"}
LOGDIR = "/tmp/tp2"


def broker(nome, duration, evict_after, peers=()):
    linhas = [f"# {nome} — em {IP[nome]}",
              f"bind = {IP[nome]}",
              f"control_port = {CONTROL_PORT}",
              f"data_port = {DATA_PORT}",
              f"duration = {duration}",
              f"evict_after = {evict_after}"]
    for p in peers:
        linhas.append(f"peer = {IP[p]}:{CONTROL_PORT}   # {p}")
    return linhas


def publisher(cen, nome, broker_nome, start_after, drain, fluxos):
    linhas = [f"# {nome} — liga-se a {broker_nome}",
              f"broker_control = {IP[broker_nome]}:{CONTROL_PORT}",
              f"log = {LOGDIR}/{cen}/{nome}.log",
              f"start_after = {start_after}",
              f"drain = {drain}"]
    for topico, qos, count, rate in fluxos:
        linhas.append(f"publish = {topico} qos={qos} count={count} rate={rate}")
    return linhas


def subscriber(cen, nome, broker_nome, duration, subs=(), acoes=()):
    linhas = [f"# {nome} — liga-se a {broker_nome}",
              f"broker_control = {IP[broker_nome]}:{CONTROL_PORT}",
              f"log = {LOGDIR}/{cen}/{nome}.log",
              f"duration = {duration}"]
    for filtro, qos in subs:
        linhas.append(f"subscribe = {filtro} qos={qos}")
    for acao in acoes:
        if acao[1] == "subscribe":
            linhas.append(f"at = {acao[0]} subscribe={acao[2]} qos={acao[3]}")
        else:
            linhas.append(f"at = {acao[0]} unsubscribe={acao[2]}")
    return linhas


CENARIOS = {}

# ------------------------------------------------------------------ fase 1
# Uma publicação de ponta a ponta, sem perdas: o caminho existe.
c = "f1_basico"
CENARIOS[c] = {
    "_descricao": "Fase 1: 1 publicador, 1 broker, 1 subscritor, G1, sem perdas.",
    "broker1": broker("broker1", duration=30, evict_after=5),
    "pub1": publisher(c, "pub1", "broker1", start_after=3, drain=10,
                      fluxos=[("north/station7/temperature", 1, 10, 2)]),
    "sub1": subscriber(c, "sub1", "broker1", duration=25,
                       subs=[("north/station7/temperature", 1)]),
}

# ------------------------------------------------------------------ fase 2
# Muitas publicações em trânsito; correr com perda em L1 (./perda.sh L1 10).
c = "f2_fiabilidade"
CENARIOS[c] = {
    "_descricao": "Fase 2: burst de 400 publicações G1 a 20/s; correr com ./perda.sh L1 10.",
    "broker1": broker("broker1", duration=50, evict_after=5),
    "pub1": publisher(c, "pub1", "broker1", start_after=3, drain=15,
                      fluxos=[("north/station7/temperature", 1, 400, 20)]),
    "sub1": subscriber(c, "sub1", "broker1", duration=45,
                       subs=[("north/station7/temperature", 1)]),
}

# Expulsão: congelar o sub2 com ./sinal.sh sub2 STOP durante mais (ou menos) que evict_after.
c = "f2_expulsao"
CENARIOS[c] = {
    "_descricao": ("Fase 2/3: sub2 é congelado com SIGSTOP. Menos de 5 s → mantém a sessão e "
                   "recebe tudo; mais de 5 s → é expulso. O sub1 não pode atrasar-se."),
    "broker1": broker("broker1", duration=55, evict_after=5),
    "pub1": publisher(c, "pub1", "broker1", start_after=3, drain=15,
                      fluxos=[("north/station7/temperature", 1, 300, 10)]),
    "sub1": subscriber(c, "sub1", "broker1", duration=50,
                       subs=[("north/station7/temperature", 1)]),
    "sub2": subscriber(c, "sub2", "broker1", duration=50,
                       subs=[("north/station7/temperature", 1)]),
}

# ------------------------------------------------------------------ fase 3
c = "f3_fanout_g2"
CENARIOS[c] = {
    "_descricao": ("Fase 3: G1 vs G2, fan-out para 6 subscritores, seletividade, filtros "
                   "sobrepostos (sub3), um tópico que ninguém subscreve, e at (sub6). "
                   "Correr com e sem ./perda.sh L1 10."),
    "broker1": broker("broker1", duration=60, evict_after=5),
    "pub1": publisher(c, "pub1", "broker1", start_after=3, drain=15, fluxos=[
        ("north/station7/temperature", 1, 300, 10),
        ("north/station7/humidity", 2, 150, 5)]),
    "pub2": publisher(c, "pub2", "broker1", start_after=3, drain=15, fluxos=[
        ("north/station8/temperature", 2, 150, 5)]),
    "pub3": publisher(c, "pub3", "broker1", start_after=3, drain=15, fluxos=[
        ("south/station1/particles", 1, 150, 5),
        ("south/station2/ignition", 2, 30, 1),
        ("west/station9/temperature", 1, 100, 5)]),   # ninguém subscreve
    # só G2: nunca pode ter duplicados
    "sub1": subscriber(c, "sub1", "broker1", 50, subs=[("north/station7/humidity", 2)]),
    # só G1: com perdas, deve acabar com algum duplicado
    "sub2": subscriber(c, "sub2", "broker1", 50, subs=[("north/station7/temperature", 1)]),
    # filtros sobrepostos: uma cópia por publicação; humidity em G2, temperature em G1
    "sub3": subscriber(c, "sub3", "broker1", 50, subs=[
        ("north/station7/temperature", 1), ("north/station7/*", 2)]),
    # prefixo de região: station7 temp (G1), humidity e station8 (G2)
    "sub4": subscriber(c, "sub4", "broker1", 50, subs=[("north/*", 2)]),
    # subscrição G1 sobre fluxo G2 (ignition): a entrega é G1, a mais fraca
    "sub5": subscriber(c, "sub5", "broker1", 50, subs=[("south/*", 1)]),
    # sem subscrições no arranque; subscreve aos 12 s e cancela aos 24 s
    "sub6": subscriber(c, "sub6", "broker1", 50, acoes=[
        (12, "subscribe", "north/station8/temperature", 2),
        (24, "unsubscribe", "north/station8/temperature")]),
}

# ------------------------------------------------------------------ fase 4
c = "f4_tres_brokers"
CENARIOS[c] = {
    "_descricao": ("Fase 4: broker1 — broker2 — broker3 em linha. Publicações em broker1 e "
                   "broker3, subscritores nos três; west/... não é subscrito por ninguém "
                   "(custo do flooding); sub6 subscreve em broker3 depois do arranque."),
    "broker1": broker("broker1", 70, 5, peers=["broker2"]),
    "broker2": broker("broker2", 70, 5, peers=["broker1", "broker3"]),
    "broker3": broker("broker3", 70, 5, peers=["broker2"]),
    "pub1": publisher(c, "pub1", "broker1", start_after=5, drain=15, fluxos=[
        ("north/station7/temperature", 1, 300, 10),
        ("north/station7/humidity", 2, 150, 5)]),
    "pub2": publisher(c, "pub2", "broker3", start_after=5, drain=15, fluxos=[
        ("south/station1/particles", 2, 150, 5)]),
    "pub3": publisher(c, "pub3", "broker1", start_after=5, drain=15, fluxos=[
        ("west/station9/temperature", 1, 150, 5)]),   # ninguém subscreve
    "sub1": subscriber(c, "sub1", "broker1", 60, subs=[("north/station7/*", 2)]),
    "sub2": subscriber(c, "sub2", "broker2", 60, subs=[("north/*", 2)]),
    "sub3": subscriber(c, "sub3", "broker3", 60, subs=[("north/station7/humidity", 2)]),
    "sub4": subscriber(c, "sub4", "broker3", 60, subs=[("south/*", 1)]),
    "sub5": subscriber(c, "sub5", "broker1", 60, subs=[("south/station1/particles", 2)]),
    "sub6": subscriber(c, "sub6", "broker3", 60, acoes=[
        (15, "subscribe", "north/station7/temperature", 1),
        (30, "unsubscribe", "north/station7/temperature")]),
}


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    base = os.path.join(here, "cenarios")
    shutil.rmtree(base, ignore_errors=True)
    for nome, nos in CENARIOS.items():
        pasta = os.path.join(base, nome)
        os.makedirs(pasta)
        with open(os.path.join(pasta, "LEIAME.txt"), "w", newline="\n") as f:
            f.write(nos["_descricao"] + "\n")
        for no, linhas in nos.items():
            if no.startswith("_"):
                continue
            with open(os.path.join(pasta, f"{no}.conf"), "w", newline="\n") as f:
                f.write("\n".join(linhas) + "\n")
        print(f"{nome}: {len(nos) - 1} configurações")
