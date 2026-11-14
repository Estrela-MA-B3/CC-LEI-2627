#!/usr/bin/env python3
"""Gera a topologia do TP2 (secção 5.1 do enunciado) para o CORE 7.x,
em dois formatos: tp2.xml (cenário XML do core-daemon) e tp2.imn (GUI Tcl)."""

# ---------------------------------------------------------------- nós
# id: (nome, tipo, x, y)   tipo: router | PC | SWITCH
NODES = {
    1: ("r1", "router", 320, 250),
    2: ("r2", "router", 720, 250),
    3: ("r3", "router", 720, 550),
    4: ("r4", "router", 320, 550),
    5: ("sw1", "SWITCH", 180, 130),
    6: ("sw2", "SWITCH", 180, 420),
    7: ("sw3", "SWITCH", 870, 130),
    8: ("sw4", "SWITCH", 870, 670),
    9: ("sw5", "SWITCH", 180, 680),
    10: ("pub1", "PC", 60, 60),
    11: ("pub2", "PC", 60, 130),
    12: ("pub3", "PC", 60, 200),
    13: ("sub1", "PC", 60, 270),
    14: ("sub2", "PC", 60, 330),
    15: ("sub3", "PC", 60, 390),
    16: ("sub4", "PC", 60, 450),
    17: ("sub5", "PC", 60, 510),
    18: ("sub6", "PC", 60, 570),
    19: ("broker1", "PC", 970, 130),
    20: ("broker2", "PC", 970, 670),
    21: ("broker3", "PC", 60, 680),
}

# ---------------------------------------------------------------- ligações
# (nó_a, ip_a, nó_b, ip_b)   ip None = lado do switch
LINKS = [
    # anel: extremidade a em .1, b em .2 (Tabela 3)
    (1, "10.0.11.1", 2, "10.0.11.2"),   # L1 r1-r2
    (2, "10.0.12.1", 3, "10.0.12.2"),   # L2 r2-r3
    (3, "10.0.13.1", 4, "10.0.13.2"),   # L3 r3-r4
    (4, "10.0.14.1", 1, "10.0.14.2"),   # L4 r4-r1
    # routers às LAN
    (1, "10.0.1.1", 5, None),
    (1, "10.0.2.1", 6, None),
    (2, "10.0.3.1", 7, None),
    (3, "10.0.4.1", 8, None),
    (4, "10.0.5.1", 9, None),
    # hosts (Tabela 2)
    (10, "10.0.1.10", 5, None),
    (11, "10.0.1.11", 5, None),
    (12, "10.0.1.12", 5, None),
    (13, "10.0.2.31", 6, None),
    (14, "10.0.2.32", 6, None),
    (15, "10.0.2.33", 6, None),
    (16, "10.0.2.34", 6, None),
    (17, "10.0.2.35", 6, None),
    (18, "10.0.2.36", 6, None),
    (19, "10.0.3.20", 7, None),
    (20, "10.0.4.20", 8, None),
    (21, "10.0.5.20", 9, None),
]

SERVICES = {
    "router": ["zebra", "OSPFv2", "OSPFv3", "IPForward"],
    "PC": ["DefaultRoute"],
}

# índice da próxima interface de cada nó
next_if = {n: 0 for n in NODES}
# para cada ligação: (if_a, if_b)
link_ifs = []
for a, ipa, b, ipb in LINKS:
    ia = next_if[a]; next_if[a] += 1
    ib = next_if[b]; next_if[b] += 1
    link_ifs.append((ia, ib))


def mac(n: int, i: int) -> str:
    return f"00:00:00:aa:{n:02x}:{i:02x}"


# ================================================================ XML
def gerar_xml() -> str:
    out = ["<?xml version='1.0' encoding='UTF-8'?>",
           '<scenario name="tp2.xml">', "  <networks>"]
    for nid, (name, kind, x, y) in NODES.items():
        if kind == "SWITCH":
            out.append(f'    <network id="{nid}" name="{name}" type="SWITCH">')
            out.append(f'      <position x="{x}.0" y="{y}.0"/>')
            out.append("    </network>")
    out += ["  </networks>", "  <devices>"]
    for nid, (name, kind, x, y) in NODES.items():
        if kind == "SWITCH":
            continue
        out.append(f'    <device id="{nid}" name="{name}" type="{kind}">')
        out.append(f'      <position x="{x}.0" y="{y}.0"/>')
        out.append("      <services>")
        for s in SERVICES[kind]:
            out.append(f'        <service name="{s}"/>')
        out.append("      </services>")
        out.append("    </device>")
    out += ["  </devices>", "  <links>"]
    for (a, ipa, b, ipb), (ia, ib) in zip(LINKS, link_ifs):
        out.append(f'    <link node_one="{a}" node_two="{b}">')
        out.append(f'      <interface_one id="{ia}" name="eth{ia}" mac="{mac(a, ia)}" '
                   f'ip4="{ipa}" ip4_mask="24"/>')
        if ipb is not None:
            out.append(f'      <interface_two id="{ib}" name="eth{ib}" mac="{mac(b, ib)}" '
                       f'ip4="{ipb}" ip4_mask="24"/>')
        out.append('      <options delay="0" bandwidth="0" per="0.0" dup="0" jitter="0" '
                   'type="1" unidirectional="0"/>')
        out.append("    </link>")
    out += ["  </links>", "</scenario>", ""]
    return "\n".join(out)


# ================================================================ IMN
def gerar_imn() -> str:
    # interfaces de cada nó: lista de (nome_if, peer, ip)
    ifaces: dict[int, list[tuple[str, int, str | None]]] = {n: [] for n in NODES}
    for (a, ipa, b, ipb), (ia, ib) in zip(LINKS, link_ifs):
        pa = "e" if NODES[a][1] == "SWITCH" else "eth"
        pb = "e" if NODES[b][1] == "SWITCH" else "eth"
        ifaces[a].append((f"{pa}{ia}", b, ipa))
        ifaces[b].append((f"{pb}{ib}", a, ipb))

    out = []
    for nid, (name, kind, x, y) in NODES.items():
        out.append(f"node n{nid} {{")
        if kind == "SWITCH":
            out.append("    type lanswitch")
        else:
            out.append("    type router")
            out.append(f"    model {kind}")
        out.append("    network-config {")
        out.append(f"\thostname {name}")
        out.append("\t!")
        if kind != "SWITCH":
            for ifn, _, ip in ifaces[nid]:
                out.append(f"\tinterface {ifn}")
                out.append(f"\t ip address {ip}/24")
                out.append("\t!")
        out.append("    }")
        out.append("    canvas c1")
        out.append(f"    iconcoords {{{x}.0 {y}.0}}")
        out.append(f"    labelcoords {{{x}.0 {y + 28}.0}}")
        for ifn, peer, _ in ifaces[nid]:
            out.append(f"    interface-peer {{{ifn} n{peer}}}")
        out.append("}")
        out.append("")
    for k, (a, _, b, _) in enumerate(LINKS, start=1):
        out += [f"link l{k} {{", f"    nodes {{n{a} n{b}}}", "    bandwidth 0", "}", ""]
    out += [
        "canvas c1 {", "    name {Canvas1}", "}", "",
        "option global {",
        "    interface_names no", "    ip_addresses yes", "    ipv6_addresses no",
        "    node_labels yes", "    link_labels yes", "    show_api no",
        "    background_images no", "    annotations yes", "    grid yes",
        "    traffic_start 0", "}", "",
        "option session {", "}", "",
    ]
    return "\n".join(out)


if __name__ == "__main__":
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "tp2.xml"), "w", newline="\n") as f:
        f.write(gerar_xml())
    with open(os.path.join(here, "tp2.imn"), "w", newline="\n") as f:
        f.write(gerar_imn())
    print("tp2.xml e tp2.imn gerados")
