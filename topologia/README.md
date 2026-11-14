# topologia/ — rede do CORE e cenários de teste

Tudo o que é preciso para testar o sistema na topologia da secção 5.1 do enunciado.
Nada aqui mexe em `src/`.

| Ficheiro | Para quê |
|---|---|
| `tp2.xml`, `tp2.imn` | A topologia (4 routers em anel, 5 LAN, 12 hosts) para o CORE 7.5 |
| `gerar_topologia.py` | Gera `tp2.xml` e `tp2.imn` |
| `gerar_cenarios.py` | Gera as configurações de `cenarios/` (mudar aqui, não à mão) |
| `cenarios/<nome>/` | Uma configuração por nó (`broker1.conf`, `pub1.conf`, …) e um `LEIAME.txt` |
| `lancar.sh` | Corre um cenário nos nós da sessão do CORE |
| `perda.sh` | Põe/retira perda e atraso (netem) nas ligações L1–L4 |
| `sinal.sh` | SIGSTOP/SIGCONT a um subscritor (simula um que deixa de responder) |

## Uso

O repositório tem de estar numa pasta da VM (os nós do CORE veem o sistema de ficheiros dela).

```bash
# 1. abrir topologia/tp2.xml no CORE, carregar no botão verde, esperar ~1 min (OSPF)
cd CC-LEI-2627/topologia
chmod +x *.sh ../run.sh

# 2. correr um cenário
./lancar.sh f1_basico               # ids pub1, sub1, broker1 ...
./lancar.sh f1_basico --ids num     # ids 1, 1, 1 ... (o src/ atual só aceita inteiros)

# 3. com perdas
./perda.sh L1 10                    # 10% de perda em L1, nos dois sentidos
./lancar.sh f2_fiabilidade
./perda.sh limpar

# 4. expulsão: noutro terminal, ~10 s depois de lançar
./lancar.sh f2_expulsao
./sinal.sh sub2 STOP 3              # 3 s < evict_after: tem de continuar a receber tudo
./sinal.sh sub2 STOP 8              # 8 s > evict_after: tem de ser expulso
```

Os registos ficam em `/tmp/tp2/<cenario>/<no>.log` e a saída/erros de cada processo em `<no>.out`.

## Cenários

| Cenário | Fase | O que põe à prova |
|---|---|---|
| `f1_basico` | 1 | 10 publicações G1 de ponta a ponta, sem perdas |
| `f2_fiabilidade` | 2 | 400 publicações a 20/s — muitas em trânsito; correr com `perda.sh L1 10` |
| `f2_expulsao` | 2–3 | `evict_after`, e que um subscritor parado não atrasa o outro |
| `f3_fanout_g2` | 3 | G1 vs G2, fan-out para 6, seletividade, filtros sobrepostos (sub3), tópico sem subscritores, `at` (sub6) |
| `f4_tres_brokers` | 4 | broker1 — broker2 — broker3, subscritor a 2 brokers de distância, subscrição tardia em broker3 |

## Capturas

Para ver o protocolo no fio: botão direito numa ligação (ex.: L1) no CORE → *Wireshark*,
ou num nó `tcpdump -i eth0 -w /tmp/tp2/l1.pcap`.
