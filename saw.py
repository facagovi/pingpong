import argparse
from socket import * #importa lib de sock python
from time import time #importa the world p/ rtt

parser = argparse.ArgumentParser()
parser.add_argument("--group", default="grupo11")
parser.add_argument("--file", default="small")
parser.add_argument("--server", default="bruh")
args = parser.parse_args()

def sfin():
    pacot.settimeout(10) #10s pra resposta
    init = time() #inicia contagem de tempo

    try:
        pacot.sendto(f"FIN|seq={i}".encode(), srdt) # envia pro server
        dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer
        rtt =(time() - init) * 1000 #calcula rtt

        if dado.decode("utf-8") != "OK":
            raise ValueError(f"Resposta FIN inesperada: {dado!r}")

        print(f"    Resposta FIN recebida: {dado.decode('utf-8')}")

    except timeout:
        print('Ja deu neh')
    except ValueError as erro:
        print(f"    {erro}")

# configs do server
serverName = '137.131.178.229' # ip do rdtunb
serverPort = 8080 #porta do rdtunb
srdt = (serverName, serverPort) #server do rdtunb
pacot = socket(AF_INET, SOCK_DGRAM) #init socket cliente

# variaveis programa/csv
retransmitidos = 0
bytearray = []

# Hello init
pack = f"HELLO|grupo={args.group}|segment_size=512|file={args.file}"
pacot.sendto(pack.encode(), srdt) # envia pro server
dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer

campos = dado.decode("utf-8").split("|") #separa campos da resposta

#salva campos da resposta
# OK= campo[0] whatever
file_size, size = campos[1].split("=")
checksum, md5 = campos[2].split("=")
#seed, <seed> = campos[3].split("=")
total_segments, n = campos[4].split("=")
segment_size, bys = campos[5].split("=")

#loop de requisições
for i in range(int(n)):
    nncolocado = True

    #vamo ficar tentando requisitar o mesmo pacote até ele ser colocado no array de bytes
    while True:
        try:
            pacot.settimeout(10) #10s pra resposta
            init = time() #inicia contagem de tempo

            # SEQ={n}
            pacot.sendto(f"REQ|seq={i}".encode(), srdt) # envia req de dado
            rtt =(time() - init) * 1000 #calcula rtt
            dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer

            data = dado.decode("utf-8").split("|") #separa campos da resposta

            #salva campos da data
            # DATA= campo[0] whatever
            sequential, num = campos[1].split("=")
            total, bys = campos[2].split("=")
            payload = campos[3].split("=")

            #aloca data dentro do array de bytes, UM de cada vez, e só se ele ja nn tiver la dentro
            if num == str(i) and nncolocado:
                bytearray.append(payload[1]) #salva payload no array
                nncolocado = False
            elif num != str(i):
                #manda ack do pacote que ja foi colocado no array, pq o server nn sabe se ele ja foi colocado ou nn
                pacot.sendto(f"ACK|seq={i}".encode(), srdt) # envia ack pro server
                continue

            # ACK
            if not nncolocado: #Ou seja, payload recebeu tranquilo
                pacot.sendto(f"ACK|seq={i}".encode(), srdt) # envia ack pro server

        #se der timeout, retransmite a mesma requisição
        except timeout:
            print(f"    Timeout na requisição {i}, retransmitindo...")
            retransmitidos += 1
            continue


pacot.close() #fecha conexao socket client