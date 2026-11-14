#!/bin/bash
# Envia um sinal ao grupo de processos de um papel a correr num nó.
# Serve para simular um subscritor que deixa de responder sem fechar a sessão.
#
#   ./sinal.sh sub2 STOP            congela o sub2
#   ./sinal.sh sub2 CONT            retoma o sub2
#   ./sinal.sh sub2 STOP 8          congela, espera 8 s e retoma
#   ./sinal.sh todos KILL           termina todos os processos do sistema

set -u
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
no=${1:?uso: $0 <no>|todos <STOP|CONT|TERM|KILL> [segundos]}
sinal=${2:?falta o sinal}
espera=${3:-}

if [ "$no" = "todos" ]; then
    $SUDO pkill "-$sinal" -f "src/main.py"
    exit 0
fi

pid=$(pgrep -f "src/main.py .*--config .*/$no\.conf" | head -1)
[ -n "$pid" ] || { echo "nenhum processo do $no a correr"; exit 1; }
pgid=$(ps -o pgid= -p "$pid" | tr -d ' ')

enviar() { $SUDO kill "-$1" -- "-$pgid" && echo "$(date +%T.%3N) SIG$1 -> $no (grupo $pgid)"; }

enviar "$sinal"
if [ -n "$espera" ]; then
    sleep "$espera"
    enviar CONT
fi
