from socket import * #importa lib de sock python
from time import time #importa the world p/ rtt

serverName = '137.131.178.229' # ip do rdtunb
serverPort = 8080 #porta do rdtunb
srdt = (serverName, serverPort) #server do rdtunb

pacot = socket(AF_INET, SOCK_DGRAM) #init socket cliente

# PING PONG
pacot.settimeout(5) #5s pra resposta
print("=== Tarefa 0 — RDT-UnB Explorer ===")
print(f"Servidor: {serverName}:{serverPort}")
print()
print("[1] PING")
init = time() #inicia contagem de tempo
pacot.sendto("PING".encode(), srdt) # envia pro server
try:
    dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer
    rtt =(time() - init) * 1000 #calcula rtt
    campos = dado.decode("utf-8").split("|")
    if len(campos) != 2 or campos[0] != "PONG" or not campos[1].startswith("time="):
        raise ValueError(f"Resposta PING inesperada: {dado!r}")
    chave, valor = campos[1].split("=", 1)
    time_ping = valor


    print(f"    RTT: {rtt:.1f} ms")


except timeout:
    print('Lerdou d++')
except ValueError as erro:
    print(f"    {erro}")

# HELLO
print()
print("[2] HELLO")
arquivo="small"
grupo="grupo11"
pack = f"HELLO|grupo={grupo}|segment_size=512|file={arquivo}" #Davi+Marcio+Emanuel
pacot.settimeout(5) #5s pra resposta
init = time() #inicia contagem de tempo
pacot.sendto(pack.encode(), srdt) # envia pro server
try:
    dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer
    rtt =(time() - init) * 1000 #calcula rtt

    campos = dado.decode("utf-8").split("|") #separa campos da resposta
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
pacot.settimeout(5) #5s pra resposta
init = time() #inicia contagem de tempo

try:
    pacot.sendto("REQ|seq=0".encode(), srdt) # envia pro server
    dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer
    rtt =(time() - init) * 1000 #calcula rtt


    pos = -1
    for _ in range(3):
        pos = dado.find(b"|", pos + 1)
        if pos == -1:
            raise ValueError("Cabeçalho DATA incompleto")

    cabecalho = dado[:pos + 1] # inclui o terceiro "|""
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

pacot.close() #fecha conexao socket client