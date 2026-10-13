import argparse # le argumentos da linha de comando
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


pacot = socket(AF_INET, SOCK_DGRAM) # UDP

# PING PONG
pacot.settimeout(5)
print("=== RDT-UnB ===")
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

# REQ
print()
print("[3] REQ seg=0")
pacot.settimeout(5)
init = time()

try:
    pacot.sendto("REQ|seq=0".encode(), srdt)
    dado, add = pacot.recvfrom(65507)
    rtt = (time() - init) * 1000

    # payload eh binario e pode ter '|': corta so no 3o '|'
    pos = -1
    for _ in range(3):
        pos = dado.find(b"|", pos + 1)
        if pos == -1:
            raise ValueError("Cabeçalho DATA incompleto")

    cabecalho = dado[:pos + 1]
    payload = dado[pos + 1:]
    partes = cabecalho[:-1].decode("utf-8").split("|")
    if (len(partes) != 3 or partes[0] != "DATA"
            or partes[1] != "seq=0"
            or not partes[2].startswith("total=")):
        raise ValueError(f"Resposta DATA inesperada: {cabecalho!r}")
    total = int(partes[2].split("=", 1)[1])
    if len(payload) != total:
        raise ValueError(f"Payload com {len(payload)} bytes; esperados {total}")

    primeiros8bytes = payload[:8]
    hex_str = " ".join(f"{b:02x}" for b in primeiros8bytes)

    print(f"    Payload recebido: {len(payload)} bytes")
    print(f"    Primeiros 8 bytes: {hex_str}")

except timeout:
    print('Lerdou d++')
except ValueError as erro:
    print(f"    {erro}")

pacot.close()