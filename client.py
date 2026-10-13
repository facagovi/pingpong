import argparse # le argumentos da linha de comando
from socket import * # lib de socket
from time import time # p/ medir rtt

# ARGUMENTOS
parser = argparse.ArgumentParser(description="Cliente RDT-UnB")
parser.add_argument("--server", default="137.131.178.229")
parser.add_argument("--port", type=int, default=8080)
parser.add_argument("--group", required=True) # sem --group nao roda (exigencia do enunciado)
parser.add_argument("--file", choices=["small", "medium", "large"], default="small") # recusa nome invalido
args = parser.parse_args()

serverName = args.server
serverPort = args.port
srdt = (serverName, serverPort)

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
    campos = dado.decode("utf-8").split("|")
    if len(campos) != 2 or campos[0] != "PONG" or not campos[1].startswith("time="):
        raise ValueError(f"Resposta PING inesperada: {dado!r}")
    print(f"    RTT: {rtt:.1f} ms")
except timeout:
    print('Lerdou d++')
except ValueError as erro:
    print(f"    {erro}")

# HELLO
print()
print("[2] HELLO")
arquivo = args.file
grupo = args.group
pack = f"HELLO|grupo={grupo}|segment_size=512|file={arquivo}" # Davi+Marcio+Emanuel
pacot.settimeout(5)
init = time()
pacot.sendto(pack.encode(), srdt)
try:
    dado, add = pacot.recvfrom(65507)
    rtt = (time() - init) * 1000

    campos = dado.decode("utf-8").split("|")
    if len(campos) < 6 or campos[0] != "OK":
        raise ValueError(f"Resposta HELLO inesperada: {dado!r}")
    file_size, tam_filesize = campos[1].split("=", 1)
    checksum, rsp_checksum = campos[2].split("=", 1)
    total_segments, rsp_totalsegments = campos[4].split("=", 1)
    segment_size, rsp_segmentsize = campos[5].split("=", 1)
    if (file_size, checksum, total_segments, segment_size) != (
        "file_size", "checksum", "total_segments", "segment_size"
    ):
        raise ValueError("Campos HELLO inesperados")

    print(f"    {'Arquivo:':<18}{arquivo} ({tam_filesize} bytes = {int(tam_filesize) // 1024} KB)")
    print(f"    {'Checksum MD5:':<18}{rsp_checksum}")
    print(f"    {'Total segmentos:':<18}{rsp_totalsegments}")
    print(f"    {'Tamanho segmento:':<18}{rsp_segmentsize} bytes")

except timeout:
    print('Lerdou d++')
except ValueError as erro:
    print(f"    {erro}")

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