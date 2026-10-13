import argparse # le argumentos da linha de comando
import csv # gravar metricas
import hashlib # MD5 (permitido pelo enunciado)
import sys # sys.exit p/ abortar
from socket import * # lib de socket
from time import time # p/ medir rtt

# ARGUMENTOS
parser = argparse.ArgumentParser(description="Cliente RDT-UnB")
parser.add_argument("--server", default="137.131.178.229")
parser.add_argument("--port", type=int, default=8080)
parser.add_argument("--group", required=True) # sem --group nao roda (exigencia do enunciado)
parser.add_argument("--file", choices=["small", "medium", "large"], default="small") # recusa nome invalido
parser.add_argument("--timeout", type=float, default=1.0) # segundos ate retransmitir
parser.add_argument("--max-tentativas", type=int, default=10) # desiste depois disso
args = parser.parse_args()

serverName = args.server
serverPort = args.port
srdt = (serverName, serverPort)


def campos(msg):
    # 'OK|a=1|b=2' -> ('OK', {'a': '1', 'b': '2'})  le por NOME, ordem nao importa
    partes = msg.split("|")
    valores = {}
    for p in partes[1:]:
        if "=" in p:
            chave, valor = p.split("=", 1)
            valores[chave] = valor
    return partes[0], valores


def separar_data(dado):
    # b'DATA|seq=N|total=T|<payload>' -> (N, payload)   ou None se nao for DATA valido
    # payload eh binario e pode ter '|': corta so no 3o '|'
    pos = -1
    for _ in range(3):
        pos = dado.find(b"|", pos + 1)
        if pos == -1:
            return None
    try:
        partes = dado[:pos].decode("ascii").split("|")
        if len(partes) != 3 or partes[0] != "DATA":
            return None
        seq = int(partes[1].split("=", 1)[1])
        total = int(partes[2].split("=", 1)[1])
    except (UnicodeDecodeError, ValueError, IndexError):
        return None
    payload = dado[pos + 1:]
    if len(payload) != total: # veio cortado -> trata como perdido
        return None
    return seq, payload


def esperar_data(seq, prazo):
    # espera o DATA do seq pedido ate 'prazo'. None = timeout
    while True:
        restante = prazo - time() # tempo que SOBRA, nao reinicia o timeout
        if restante <= 0:
            return None
        pacot.settimeout(restante)
        try:
            dado, add = pacot.recvfrom(65507)
        except timeout:
            return None
        r = separar_data(dado)
        if r is not None and r[0] == seq:
            return r[1]
        # seq errado = DATA atrasado de REQ antigo: descarta e segue esperando


pacot = socket(AF_INET, SOCK_DGRAM) # UDP

# PING PONG
pacot.settimeout(5)
print("=== RDT-UnB  Stop-and-Wait ===")
print(f"Servidor: {serverName}:{serverPort}")
print()
print("[1] PING")
init = time()
pacot.sendto("PING".encode(), srdt)
try:
    dado, add = pacot.recvfrom(65507) # 65507 = maior datagrama UDP
    rtt = (time() - init) * 1000
    tipo, v = campos(dado.decode("utf-8"))
    if tipo != "PONG" or "time" not in v:
        raise ValueError(f"Resposta PING inesperada: {dado!r}")
    print(f"    RTT: {rtt:.1f} ms")
except timeout:
    print('Lerdou d++')
except ValueError as erro:
    print(f"    {erro}")

# HELLO (com retransmissao)
print()
print("[2] HELLO")
arquivo = args.file
grupo = args.group
pack = f"HELLO|grupo={grupo}|segment_size=512|file={arquivo}" # Davi+Marcio+Emanuel
pacot.settimeout(args.timeout)

v = None
for tentativa in range(1, args.max_tentativas + 1):
    pacot.sendto(pack.encode(), srdt)
    try:
        dado, add = pacot.recvfrom(65507)
        tipo, resp = campos(dado.decode("utf-8", errors="replace"))
        if tipo == "OK":
            v = resp
            break
        print(f"    ignorei pacote que nao eh OK: {dado[:30]!r}") # ex: PONG atrasado
    except timeout:
        print(f"    timeout no HELLO (tentativa {tentativa}), reenviando") # UDP perdeu: manda de novo

if v is None:
    sys.exit("Servidor nao respondeu ao HELLO. Abortando.")

try:
    file_size = int(v["file_size"])
    md5_servidor = v["checksum"] # guardar p/ comparar no fim
    total_seg = int(v["total_segments"])
    seg_size = int(v["segment_size"]) # servidor pode ajustar o que pedimos
except (KeyError, ValueError):
    sys.exit(f"OK mal formado: {v}")

print(f"    {'Arquivo:':<18}{arquivo} ({file_size} bytes = {file_size // 1024} KB)")
print(f"    {'Checksum MD5:':<18}{md5_servidor}")
print(f"    {'Seed:':<18}{v.get('seed', '?')}")
print(f"    {'Total segmentos:':<18}{total_seg}")
print(f"    {'Tamanho segmento:':<18}{seg_size} bytes")

# STOP-AND-WAIT: REQ -> DATA -> ACK, um segmento por vez
print()
print("[3] Transferencia REQ -> DATA -> ACK")
pedacos = [] # payloads em ordem
metricas = [] # uma linha do CSV por segmento
total_retx = 0
bytes_recebidos = 0
t_inicio = time()

for seq in range(total_seg):
    req = f"REQ|seq={seq}".encode()
    retx = 0 # retransmissoes DESTE segmento
    t_primeiro_envio = time() # inicio do segmento (p/ throughput)

    while True:
        t_envio = time() # envio MAIS RECENTE (p/ rtt)
        pacot.sendto(req, srdt)
        payload = esperar_data(seq, t_envio + args.timeout)
        if payload is not None:
            break # chegou certo
        retx += 1 # timeout: manda o REQ de novo
        if retx > args.max_tentativas:
            sys.exit(f"Segmento {seq}: sem resposta apos {args.max_tentativas} retransmissoes. Abortando.")

    t_chegada = time()
    pacot.sendto(f"ACK|seq={seq}".encode(), srdt) # confirma, so depois pede o proximo
    pedacos.append(payload)
    total_retx += retx
    bytes_recebidos += len(payload)

    # rtt: ultimo REQ -> DATA. se retransmitiu, eh ambiguo (Karn): marca rtt_valido=0
    rtt = t_chegada - t_envio
    # throughput: bytes uteis / tempo TOTAL do segmento (inclui timeouts)
    duracao = t_chegada - t_primeiro_envio
    decorrido = t_chegada - t_inicio
    metricas.append({
        "seq": seq,
        "bytes": len(payload),
        "rtt_ms": round(rtt * 1000, 3),
        "rtt_valido": 1 if retx == 0 else 0,
        "throughput_kbps": round(len(payload) * 8 / duracao / 1000, 3),
        "throughput_acumulado_kbps": round(bytes_recebidos * 8 / decorrido / 1000, 3),
        "retransmissoes": retx,
        "timeout_ocorrido": 1 if retx > 0 else 0,
        "tempo_decorrido_s": round(decorrido, 4),
    })

    if seq % 200 == 0 or seq == total_seg - 1: # progresso sem poluir a tela
        print(f"    seg {seq + 1}/{total_seg} ({100 * (seq + 1) / total_seg:5.1f}%)  "
              f"rtt {rtt * 1000:6.1f} ms  retx {retx}")

tempo_total = time() - t_inicio
dados = b"".join(pedacos)

# so amostras sem retransmissao entram na media (Karn)
rtts_validos = [m["rtt_ms"] for m in metricas if m["rtt_valido"] == 1]
rtt_medio = sum(rtts_validos) / len(rtts_validos) if rtts_validos else 0
throughput_medido = file_size * 8 / tempo_total / 1000 # kbit/s

print()
print(f"    Recebidos:         {len(dados)} de {file_size} bytes")
print(f"    Tempo total:       {tempo_total:.2f} s")
print(f"    Retransmissoes:    {total_retx}")
print(f"    Segs com timeout:  {sum(m['timeout_ocorrido'] for m in metricas)}")
print(f"    RTT medio:         {rtt_medio:.2f} ms ({len(rtts_validos)} amostras validas)")
print(f"    Throughput medido: {throughput_medido:.1f} kbit/s")

# CSV
nome_csv = f"{grupo}_{arquivo}_saw.csv"
with open(nome_csv, "w", newline="") as f: # newline="" evita linha em branco no Windows
    w = csv.DictWriter(f, fieldnames=list(metricas[0].keys()))
    w.writeheader()
    w.writerows(metricas)
print(f"    Metricas salvas em {nome_csv}")

# MD5 + FIN/DONE
print()
print("[4] Verificacao MD5 + FIN")
md5_local = hashlib.md5(dados).hexdigest() # md5 do que NOS recebemos
md5_bate = md5_local == md5_servidor.lower()
print(f"    MD5 esperado: {md5_servidor}")
print(f"    MD5 recebido: {md5_local}  -> {'CONFERE' if md5_bate else 'DIFERENTE'}")

fin = f"FIN|checksum={md5_local}".encode() # manda o NOSSO md5, servidor que julga
ok_servidor = None # None = servidor nao respondeu
for tentativa in range(1, args.max_tentativas + 1):
    pacot.sendto(fin, srdt)
    prazo = time() + args.timeout
    while ok_servidor is None:
        restante = prazo - time()
        if restante <= 0:
            break
        pacot.settimeout(restante)
        try:
            dado, add = pacot.recvfrom(65507)
        except timeout:
            break
        if dado.startswith(b"DONE|"):
            tipo, resp = campos(dado.decode("utf-8", errors="replace"))
            ok_servidor = resp.get("ok") == "1"
        # qualquer outra coisa (DATA atrasado) ignora
    if ok_servidor is not None:
        break
    print(f"    timeout no FIN (tentativa {tentativa}), reenviando")

if ok_servidor is None:
    print("    Servidor nao respondeu ao FIN")
else:
    print(f"    DONE do servidor: ok={1 if ok_servidor else 0}")

pacot.close()

# salva o arquivo recebido
nome_saida = f"{grupo}_{arquivo}.bin"
with open(nome_saida, "wb") as f: # "wb" = escrita binaria
    f.write(dados)
print(f"    Arquivo salvo em {nome_saida}")

sys.exit(0 if md5_bate else 1) # codigo de saida != 0 se o arquivo veio errado