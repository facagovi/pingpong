import argparse
import csv
from datetime import datetime, timezone
import hashlib
import random
from socket import *
from time import perf_counter as time

# Argumentos de linha de comando exigidos pelas Entregas 1 e 2
parser = argparse.ArgumentParser(description="Cliente RDT-UnB - Entregas 1 e 2")
parser.add_argument("--server", default="137.131.178.229", help="IP do servidor")
parser.add_argument("--port", type=int, default=8080, help="Porta UDP do servidor")
parser.add_argument("--group", required=True, help="Identificador do grupo (ex: grupo11)")
parser.add_argument("--file", default="small", choices=["small", "medium", "large"], help="Arquivo a baixar")
parser.add_argument("--protocol", default="saw", choices=["saw", "gbn", "sr"], help="Protocolo de transporte")
parser.add_argument("--window", type=int, default=1, choices=[1, 4, 8, 16], help="Tamanho da janela (N)")
parser.add_argument("--loss", type=float, default=0.0, choices=[0.0, 0.05, 0.10], help="Taxa de perda simulada (0, 0.05 ou 0.10)")
args = parser.parse_args()

# Ajuste obrigatório da janela para Stop-and-Wait
if args.protocol == "saw":
    args.window = 1

srdt = (args.server, args.port)
pacot = socket(AF_INET, SOCK_DGRAM)
pacot.settimeout(1.0)  # Timeout base de 1 segundo

# Handshake HELLO / OK
print(f"[*] Iniciando sessão com {args.protocol.upper()} (Janela={args.window}, Perda={args.loss*100}%)...")
pack = f"HELLO|grupo={args.group}|segment_size=512|file={args.file}".encode()

# lança hello ate ok ser recebido mec enao timeout
while True:
    try:
        pacot.sendto(pack, srdt)
        dado, _ = pacot.recvfrom(65507)
        campos = dado.decode().split("|")
        if campos[0] == "OK":
            break
    except timeout:
        print("[!] Timeout no HELLO, retransmitindo...")

# Extração dos parâmetros da sessão do OK
# OK|file_size=<bytes>|checksum=<md5>|seed=<seed>|total_segments=<n>|segment_size=<bytes>
file_size = int(campos[1].split("=")[1])
checksum_esperado = campos[2].split("=")[1]
total_segments = int(campos[4].split("=")[1])
segment_size = int(campos[5].split("=")[1])

print(f"[*] Arquivo: {args.file} | Total de segmentos: {total_segments} | Tamanho: {file_size} bytes")

# buffer pra receber os segmentos, None = não recebido ainda
buffer_partes = [None] * total_segments

# dicionario pra armazenar metricas e transformar em CSV no final
# preenche com valores default pra armazenar depois
metricas_segmento = {}
for seq in range(total_segments):
    metricas_segmento[seq] = {
        "retransmissions": 0,
        "timeout_occurred": 0,
        "simulated_loss": 0,
        "rtt_ms": 0.0,
        "throughput_kbps": 0.0,
        "timestamp": "",
        "size_bytes": 0
    }

# STOP AND WAIT
if args.protocol == "saw":
    seq_atual = 0
    while seq_atual < total_segments:
        init_req = time()
        pacot.sendto(f"REQ|seq={seq_atual}".encode(), srdt)
        
        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = time()
            # Separa os 3 primeiros pipes sem decodificar o binário do payload
            partes = dado.split(b"|", 3)
            if partes[0] == b"DATA":
                num_seq = int(partes[1].split(b"=")[1])
                payload_bin = partes[3]

                # simula perda aleatoriamente cm taxa salvando no CSV
                if args.loss > 0.0 and random.random() < args.loss:
                    metricas_segmento[seq_atual]["simulated_loss"] = 1
                    # continua pro timeout e retransmissao do mesmo pacote
                    continue

                # Se o pacote recebido for o esperado, manda ACK mesmo nn servindo pra nada e salva no buffer
                if num_seq == seq_atual:
                    rtt = (fim_req - init_req) * 1000  # RTT em milissegundos
                    
                    buffer_partes[seq_atual] = payload_bin
                    
                    m = metricas_segmento[seq_atual]
                    m["rtt_ms"] = round(rtt, 2)
                    m["size_bytes"] = len(payload_bin)
                    m["throughput_kbps"] = round((len(payload_bin) * 8) / rtt, 2) if rtt > 0.001 else 0.0 #kbps
                    m["timestamp"] = datetime.now(timezone.utc).isoformat() #horario bonito
                    
                    seq_atual += 1

        except timeout:
            metricas_segmento[seq_atual]["timeout_occurred"] = 1
            metricas_segmento[seq_atual]["retransmissions"] += 1


# GO BACK N
elif args.protocol == "gbn":
    base = 0
    next_seq = 0
    time_envios = {}

    while base < total_segments:
        # Envia REQs até preencher a janela, simulação do gbn
        while next_seq < base + args.window and next_seq < total_segments:
            time_envios[next_seq] = time()
            pacot.sendto(f"REQ|seq={next_seq}".encode(), srdt)
            next_seq += 1

        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = time()
            partes = dado.split(b"|", 3)
            if partes[0] == b"DATA":
                num_seq = int(partes[1].split(b"=")[1])
                payload_bin = partes[3]

                # Simula perda
                if args.loss > 0.0 and random.random() < args.loss:
                    metricas_segmento[num_seq]["simulated_loss"] = 1
                    continue

                # No GBN só aceita se for exatamente a base esperada em ordem
                if num_seq == base:
                    rtt = (fim_req - time_envios[base]) * 1000
                    
                    buffer_partes[base] = payload_bin
                    
                    m = metricas_segmento[base]
                    m["rtt_ms"] = round(rtt, 2)
                    m["size_bytes"] = len(payload_bin)
                    m["throughput_kbps"] = round((len(payload_bin) * 8) / rtt, 2) if rtt > 0.001 else 0.0
                    m["timestamp"] = datetime.now(timezone.utc).isoformat()
                    
                    base += 1

        except (timeout, TimeoutError):
            # no gbn timeout da base retransmite ele e TODOS os seguintes da janela
            metricas_segmento[base]["timeout_occurred"] = 1
            for s in range(base, next_seq):
                metricas_segmento[s]["retransmissions"] += 1
            # Recua o "ponteiro" para o início da janela não confirmada
            next_seq = base


# SELECTIVE REPEAT
elif args.protocol == "sr":
    base = 0
    next_seq = 0
    time_envios = {}
    req_rcv = [False] * total_segments

    while base < total_segments:
        # Envia REQs até preencher a janela, não síncrono mas simula o SR
        while next_seq < base + args.window and next_seq < total_segments:
            time_envios[next_seq] = time()
            pacot.sendto(f"REQ|seq={next_seq}".encode(), srdt)
            next_seq += 1

        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = time()
            partes = dado.split(b"|", 3)
            if partes[0] == b"DATA":
                num_seq = int(partes[1].split(b"=")[1])
                payload_bin = partes[3]

                # Confirma individualmente com ACK mesmo se for segmento repetido antigo
                if num_seq < base:
                    pacot.sendto(f"ACK|seq={num_seq}".encode(), srdt)
                    continue

                # Simula perda
                if args.loss > 0.0 and random.random() < args.loss:
                    metricas_segmento[num_seq]["simulated_loss"] = 1
                    continue

                if base <= num_seq < base + args.window:
                    pacot.sendto(f"ACK|seq={num_seq}".encode(), srdt)
                    
                    if not req_rcv[num_seq]:
                        rtt = (fim_req - time_envios[num_seq]) * 1000 #instante certo
                        req_rcv[num_seq] = True
                        buffer_partes[num_seq] = payload_bin
                        
                        m = metricas_segmento[num_seq]
                        m["rtt_ms"] = round(rtt, 2)
                        m["size_bytes"] = len(payload_bin)
                        m["throughput_kbps"] = round((len(payload_bin) * 8) / rtt, 2) if rtt > 0.001 else 0.0
                        m["timestamp"] = datetime.now(timezone.utc).isoformat()

                    # Desliza a base da janela enquanto houver ACKs consecutivos prontos
                    while base < total_segments and req_rcv[base]:
                        base += 1

        except (timeout, TimeoutError):
            # Regra SR: retransmite APENAS o segmento em timeout (a base não confirmada)
            metricas_segmento[base]["timeout_occurred"] = 1
            metricas_segmento[base]["retransmissions"] += 1
            time_envios[base] = time()
            pacot.sendto(f"REQ|seq={base}".encode(), srdt)

# ==============================================================================
# MONTAGEM FINAL DO ARQUIVO E CHECK E HASH MD5
# ==============================================================================
arquivo_final = bytearray()
for parte in buffer_partes:
    if parte is not None:
        arquivo_final.extend(parte)

md5_calculado = hashlib.md5(arquivo_final).hexdigest()
print(f"[*] MD5 Servidor : {checksum_esperado}")
print(f"[*] MD5 Calculado: {md5_calculado}")

# Encerramento com FIN / DONE
ok_status = "0"
while True:
    try:
        pacot.sendto(f"FIN|checksum={md5_calculado}".encode("utf-8"), srdt)
        done_raw, _ = pacot.recvfrom(65507)
        
        # Ignora pacotes de DATA atrasados e só processa se for DONE
        if done_raw.startswith(b"DONE"):
            partes_done = done_raw.decode("utf-8", errors="ignore").split("|")
            ok_status = partes_done[1].split("=")[1].strip()
            break
            
    except timeout:
        print("[!] Timeout no FIN, tentando novamente...")

if ok_status == "1":
    print("[+] SUCESSO: Arquivo íntegro transferido e validado com sucesso!")
    with open(f"recebido_{args.file}.bin", "wb") as f:
        f.write(arquivo_final)
else:
    print("[-] ERRO: Checksum divergente! Servidor retornou DONE ok=0.")

pacot.close()

# ==============================================================================
# GRAVAÇÃO DO CSV DE MÉTRICAS OBRIGATÓRIAS
# ==============================================================================
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"metrics_{args.group}_{args.protocol}_{timestamp_str}.csv"

campos_csv = [
    "segment", "timestamp", "size_bytes", "rtt_ms", "rtt_estimated_ms",
    "timeout_ms", "throughput_kbps", "retransmissions", "timeout_occurred",
    "window_size", "protocol", "simulated_loss"
]

with open(csv_filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=campos_csv)
    writer.writeheader()
    for seq in range(total_segments):
        m = metricas_segmento[seq]
        writer.writerow({
            "segment": seq,
            "timestamp": m["timestamp"],
            "size_bytes": m["size_bytes"],
            "rtt_ms": m["rtt_ms"],
            "rtt_estimated_ms": 0.0,      # Deixar 0.0 para Entrega 2; será populado na Entrega 3
            "timeout_ms": 1000.0,
            "throughput_kbps": m["throughput_kbps"],
            "retransmissions": m["retransmissions"],
            "timeout_occurred": m["timeout_occurred"],
            "window_size": args.window,
            "protocol": args.protocol,
            "simulated_loss": m["simulated_loss"]
        })

print(f"[+] Métricas salvas com sucesso em: {csv_filename}")