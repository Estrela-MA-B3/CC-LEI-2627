#!/bin/bash
# Introduz perda (e, opcionalmente, atraso) nas ligações entre routers, com netem.
# Aplica-se nas duas pontas da ligação, por isso afeta os dois sentidos.
#
#   ./perda.sh L1 10          10% de perda em L1
#   ./perda.sh L1 10 20       10% de perda e 20 ms de atraso em L1
#   ./perda.sh todas 5        5% de perda em L1, L2, L3 e L4
#   ./perda.sh L1 0           retira tudo de L1
#   ./perda.sh limpar         retira tudo de todas
#   ./perda.sh estado         mostra o que está aplicado
#
# Com um só broker (fases 1 a 3), todo o tráfego passa em L1.

set -u
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
sess=$(ls -dt /tmp/pycore.*/ 2>/dev/null | head -1); sess=${sess%/}
[ -n "$sess" ] || { echo "nenhuma sessão do CORE a correr"; exit 1; }

# ligação -> rede e routers das duas pontas (Tabela 3)
declare -A REDE=([L1]=10.0.11 [L2]=10.0.12 [L3]=10.0.13 [L4]=10.0.14)
declare -A PONTAS=([L1]="r1 r2" [L2]="r2 r3" [L3]="r3 r4" [L4]="r4 r1")

no() { $SUDO vcmd -c "$sess/$1" -- "${@:2}"; }

iface() {   # <router> <prefixo de rede>  -> nome da interface com esse endereço
    no "$1" ip -o -4 addr show | awk -v p="$2." 'index($4, p) == 1 {print $2; exit}'
}

aplicar() {   # <ligacao> <perda%> [atraso_ms]
    local l=$1 perda=$2 atraso=${3:-0}
    for r in ${PONTAS[$l]}; do
        local i; i=$(iface "$r" "${REDE[$l]}")
        [ -n "$i" ] || { echo "  ! $r não tem interface em ${REDE[$l]}.0/24"; continue; }
        no "$r" tc qdisc del dev "$i" root 2>/dev/null
        if [ "$perda" != "0" ] || [ "$atraso" != "0" ]; then
            no "$r" tc qdisc add dev "$i" root netem loss "${perda}%" delay "${atraso}ms"
            echo "  $l  $r/$i: loss ${perda}% delay ${atraso}ms"
        else
            echo "  $l  $r/$i: sem perdas"
        fi
    done
}

estado() {
    for l in L1 L2 L3 L4; do
        for r in ${PONTAS[$l]}; do
            local i; i=$(iface "$r" "${REDE[$l]}")
            printf "  %s %s/%-5s %s\n" "$l" "$r" "$i" \
                "$(no "$r" tc qdisc show dev "$i" | grep netem || echo '-')"
        done
    done
}

case "${1:-}" in
    L1|L2|L3|L4) aplicar "$1" "${2:?falta a percentagem}" "${3:-0}" ;;
    todas)       for l in L1 L2 L3 L4; do aplicar $l "${2:?falta a percentagem}" "${3:-0}"; done ;;
    limpar)      for l in L1 L2 L3 L4; do aplicar $l 0; done ;;
    estado)      estado ;;
    *)           sed -n '2,12p' "$0"; exit 1 ;;
esac
