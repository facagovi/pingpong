import argparse
import hashlib
from socket import * #importa lib de sock python
from time import time

parser = argparse.ArgumentParser()
parser.add_argument("--group", default="grupo11")
parser.add_argument("--file", default="small")
parser.add_argument("--server", default="bruh")
args = parser.parse_args()

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
                continue #esse aqui é ack se deu erro

            # ACK oficial
            if not nncolocado: #Ou seja, payload recebeu tranquilo
                pacot.sendto(f"ACK|seq={i}".encode(), srdt) # envia ack pro server
                break

        #se der timeout, retransmite a mesma requisição
        except timeout:
            retransmitidos += 1
            continue #manda de novo

        break

#codifica o array de bytes em bytes e calcula o checksum md5 do arquivo recebido
msg = bytes(bytearray, 'utf-8')
md5 = hashlib.md5(msg).hexdigest()

while True:
    try:
        pacot.settimeout(10) #10s pra resposta
        pacot.sendto(f"FIN|checksum={md5}".encode(), srdt) # envia pro server

        done, add = pacot.recvfrom(65507) #resposta tankando max do buffer
        
        done = done.decode("utf-8").split("|") #separa campos da resposta

        #salva campos da data
        #done= campo[0] whatever
        ok, fim = campos[1].split("=")

    except timeout:
        continue #nn perceberam q acabou, manda de novo
    if fim == 1:
        break

if fim == 1:
    print("Arquivo recebido com sucesso!")
else:
    print("Arquivo corrompido! Checagem MD5 não confere com a do servidor.")


pacot.close() #fecha conexao socket client