#!/bin/bash
# Lança um cenário de teste nos nós da sessão do CORE que está a correr.
#
#   ./lancar.sh <cenario> [--ids nome|num]
#
#   <cenario>   pasta em cenarios/ (ex.: f1_basico)
#   --ids nome  passa --id pub1, --id sub3, ... (por omissão; é o que o enunciado sugere)
#   --ids num   passa --id 1, --id 3, ... (o código atual do src/ só aceita inteiros)
#
# Cada ficheiro <no>.conf do cenário corre no nó do CORE com esse nome:
# broker* -> broker, pub* -> publisher, sub* -> subscriber.
# Os brokers arrancam primeiro; os clientes 1 s depois, todos em paralelo.
# Registos em /tmp/tp2/<cenario>/<no>.log; stdout/stderr em <no>.out.

set -u
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)
RUN="$repo/run.sh"

cen=${1:-}
ids=nome
if [ "${2:-}" = "--ids" ]; then ids=${3:-nome}; fi

if [ -z "$cen" ] || [ ! -d "$here/cenarios/$cen" ]; then
    echo "uso: $0 <cenario> [--ids nome|num]"
    echo "cenários: $(ls "$here/cenarios" | tr '\n' ' ')"
    exit 1
fi
[ -x "$RUN" ] || { echo "não encontro $RUN executável (chmod +x run.sh?)"; exit 1; }

SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"

# sessão do CORE mais recente
sess=$(ls -dt /tmp/pycore.*/ 2>/dev/null | head -1)
sess=${sess%/}
[ -n "$sess" ] || { echo "nenhuma sessão do CORE a correr (carrega no botão verde)"; exit 1; }

out="/tmp/tp2/$cen"
$SUDO rm -rf "$out"
$SUDO mkdir -p "$out"
$SUDO chmod 777 /tmp/tp2 "$out"

lancar_no() {   # <no> <papel>
    local no=$1 papel=$2 id
    if [ "$ids" = "num" ]; then id=$(echo "$no" | tr -dc '0-9'); else id=$no; fi
    [ -S "$sess/$no" ] || { echo "  ! o nó $no não existe nesta sessão"; return; }
    echo "  $no: $papel --id $id"
    $SUDO vcmd -c "$sess/$no" -- \
        "$RUN" "$papel" --id "$id" --config "$here/cenarios/$cen/$no.conf" \
        > "$out/$no.out" 2>&1 &
}

echo "cenário $cen  (sessão $sess, ids=$ids)"
cat "$here/cenarios/$cen/LEIAME.txt" 2>/dev/null | sed 's/^/  /'

for f in "$here/cenarios/$cen"/broker*.conf; do
    [ -e "$f" ] && lancar_no "$(basename "$f" .conf)" broker
done
sleep 1
for f in "$here/cenarios/$cen"/pub*.conf; do
    [ -e "$f" ] && lancar_no "$(basename "$f" .conf)" publisher
done
for f in "$here/cenarios/$cen"/sub*.conf; do
    [ -e "$f" ] && lancar_no "$(basename "$f" .conf)" subscriber
done

# salvaguarda: se algum processo não terminar sozinho, SIGTERM e 5 s depois SIGKILL
maxdur=$(grep -hE '^(duration|drain|start_after)' "$here/cenarios/$cen"/*.conf \
         | awk -F= '{s+=0; v=$2+0; if (v>m) m=v} END {print m}')
limite=$((maxdur + 30))
echo "à espera que terminem (no máximo ${limite} s)..."
( sleep "$limite"
  if pgrep -f "$here/cenarios/$cen/" >/dev/null; then
      echo "  ! processos ainda vivos ao fim de ${limite} s: SIGTERM"
      $SUDO pkill -TERM -f "$here/cenarios/$cen/"; sleep 5
      $SUDO pkill -KILL -f "$here/cenarios/$cen/"
  fi ) &
guarda=$!
wait $(jobs -p | grep -v "^$guarda$") 2>/dev/null
kill "$guarda" 2>/dev/null

echo
echo "== resultado em $out"
for f in "$out"/*.log; do
    [ -e "$f" ] && printf "  %-14s %6d linhas\n" "$(basename "$f")" "$(wc -l < "$f")"
done
for f in "$out"/*.out; do
    if [ -s "$f" ]; then echo "  -- $(basename "$f") (saída/erros):"; head -5 "$f" | sed 's/^/     /'; fi
done
