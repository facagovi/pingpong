from socket import * #importa lib de sock python
from time import time #importa the world p/ rtt

serverName = '137.131.178.229' # ip do rdtunb
serverPort = 8080 #porta do rdtunb
srdt = (serverName, serverPort) #server do rdtunb

pacot = socket(AF_INET, SOCK_DGRAM) #init socket cliente

# PING PONG
pacot.sendto("PING".encode(), srdt) # envia pro server
try:
    pacot.settimeout(5) #5s pra resposta
    init = time() #inicia contagem de tempo
    dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer

    rtt =(time() - init) * 1000 #calcula rtt
except timeout:
    print('Lerdou d++')

# HELLO
pack = "HELLO|grupo = Damarema|segment_size = 512|file = small" #Davi+Marcio+Emanuel
pacot.sendto(pack.encode(), srdt) # envia pro server
try:
    pacot.settimeout(5) #5s pra resposta
    init = time() #inicia contagem de tempo
    dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer
    campos = dado.decode().split('|') #separa campos da resposta
    #PARSEAR A RESPOSTA

    rtt =(time() - init) * 1000 #calcula rtt
except timeout:
    print('Lerdou d++')

# REQ
pacot.sendto("PING".encode(), srdt) # envia pro server
try:
    pacot.settimeout(5) #5s pra resposta
    init = time() #inicia contagem de tempo
    dado, add = pacot.recvfrom(65507) #resposta tankando max do buffer

    rtt =(time() - init) * 1000 #calcula rtt
except timeout:
    print('Lerdou d++')

pacot.close() #fecha conexao socket client