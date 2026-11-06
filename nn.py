import argparse
import csv
from datetime import datetime, timezone
import hashlib
import random
from socket import *
from time import perf_counter

# param
parser = argparse.ArgumentParser(description="Cliente RDT-UnB - Entrega 3: GBN e SR Adaptativos")
parser.add_argument("--server", default="137.131.178.229", help="IP do servidor")
parser.add_argument("--port", type=int, default=8080, help="Porta UDP do servidor")
parser.add_argument("--group", required=True, help="Identificador do grupo (ex: grupo11)")
parser.add_argument("--file", default="small", choices=["small", "medium", "large"], help="Arquivo a baixar")
parser.add_argument("--protocol", default="gbn", choices=["gbn", "sr"], help="Protocolo de janela deslizante")
parser.add_argument("--window", type=int, default=4, choices=[4, 8, 16], help="Tamanho da janela (N)")
parser.add_argument("--loss", type=float, default=0.0, choices=[0.0, 0.05, 0.10], help="Taxa de perda simulada")
parser.add_argument("--alpha", type=float, default=0.125, help="Fator de ponderação para RTT estimado (EWMA)")
parser.add_argument("--beta", type=float, default=0.25, help="Fator de ponderação para DevRTT")

args = parser.parse_args()

srdt = (args.server, args.port)
pacot = socket(AF_INET, SOCK_DGRAM)

# EWMA
ALPHA = args.alpha
BETA = args.beta

estimated_rtt = None      # RTT_est (ms)
dev_rtt = None            # DevRTT (ms)
tempout = 1000.0 # Timeout dinâmico inicial (1000 ms)

pacot.settimeout(tempout / 1000.0)

# HELLO
print(f"[*] Iniciando {args.protocol.upper()} Adaptativo (Janela={args.window}, Perda={args.loss*100}%, alpha={ALPHA}, beta={BETA})...")
pack = f"HELLO|grupo={args.group}|segment_size=512|file={args.file}".encode("utf-8")

while True:
    try:
        pacot.sendto(pack, srdt)
        dado, _ = pacot.recvfrom(65507)
        if dado.startswith(b"OK"):
            campos = dado.decode("utf-8", errors="ignore").split("|")
            break
    except (timeout, TimeoutError):
        print("[!] Timeout no HELLO, retransmitindo...")

file_size = int(campos[1].split("=")[1])
checksum_esperado = campos[2].split("=")[1]
total_segments = int(campos[4].split("=")[1])
segment_size = int(campos[5].split("=")[1])

print(f"[*] Arquivo: {args.file} | Total de segmentos: {total_segments} | Tamanho: {file_size} bytes")

buffer_partes = [None] * total_segments
metricas_segmento = {}
for seq in range(total_segments):
    metricas_segmento[seq] = {
        "retransmissions": 0,
        "timeout_occurred": 0,
        "simulated_loss": 0,
        "rtt_ms": 0.0,
        "rtt_estimated_ms": 0.0,
        "timeout_ms": 1000.0,
        "throughput_kbps": 0.0,
        "timestamp": "",
        "size_bytes": 0
    }

def atualizar_rtt_adaptativo(rtt_amostra):
    """Atualiza RTT estimado, DevRTT e Timeout Interval com base na EWMA."""
    global estimated_rtt, dev_rtt, tempout
    if estimated_rtt is None:
        estimated_rtt = rtt_amostra
        dev_rtt = rtt_amostra / 2.0
    else:
        dev_rtt = (1.0 - BETA) * dev_rtt + BETA * abs(rtt_amostra - estimated_rtt)
        estimated_rtt = (1.0 - ALPHA) * estimated_rtt + ALPHA * rtt_amostra
    
    tempout = estimated_rtt + 4.0 * dev_rtt
    tempout = max(tempout, 50.0)  # Piso de proteção (50 ms)

# GBN
if args.protocol == "gbn":
    base = 0
    next_seq = 0
    time_envios = {}

    while base < total_segments:
        # Atualiza td hr
        pacot.settimeout(tempout / 1000.0)

        # Envia REQs até preencher a janela
        while next_seq < base + args.window and next_seq < total_segments:
            time_envios[next_seq] = perf_counter()
            pacot.sendto(f"REQ|seq={next_seq}".encode("utf-8"), srdt)
            next_seq += 1

        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = perf_counter()

            partes = dado.split(b"|", 3)
            if partes[0] == b"DATA":
                num_seq = int(partes[1].split(b"=")[1])
                payload_bin = partes[3]

                # Simula perda
                if args.loss > 0.0 and random.random() < args.loss:
                    metricas_segmento[num_seq]["simulated_loss"] = 1
                    continue

                # No GBN só aceita se for a base esperada em ordem
                if num_seq == base:
                    rtt_amostra = (fim_req - time_envios[base]) * 1000.0
                    
                    # Karn: só atualiza EWMA se o pacote não sofreu retransmissão
                    if metricas_segmento[base]["retransmissions"] == 0:
                        atualizar_rtt_adaptativo(rtt_amostra)

                    pacot.sendto(f"ACK|seq={base}".encode("utf-8"), srdt)
                    buffer_partes[base] = payload_bin

                    m = metricas_segmento[base]
                    m["rtt_ms"] = round(rtt_amostra, 2)
                    m["rtt_estimated_ms"] = round(estimated_rtt, 2) if estimated_rtt is not None else 0.0
                    m["timeout_ms"] = round(tempout, 2)
                    m["size_bytes"] = len(payload_bin)
                    m["throughput_kbps"] = round((len(payload_bin) * 8.0) / rtt_amostra, 2) if rtt_amostra > 0.001 else 0.0
                    m["timestamp"] = datetime.now(timezone.utc).isoformat()

                    base += 1
                    if base % 100 == 0:
                        print(f"\r[*] Progresso GBN: {base}/{total_segments} | Timeout: {tempout:.1f}ms", end="", flush=True)
                else:
                    if base > 0:
                        pacot.sendto(f"ACK|seq={base - 1}".encode("utf-8"), srdt)

        except (timeout, TimeoutError):
            metricas_segmento[base]["timeout_occurred"] = 1
            for s in range(base, next_seq):
                metricas_segmento[s]["retransmissions"] += 1
            # Backoff exponencial do timeout
            tempout = min(tempout * 2.0, 5000.0)
            next_seq = base

# SR
elif args.protocol == "sr":
    base = 0
    next_seq = 0
    time_envios = {}
    ack_recebido = [False] * total_segments

    while base < total_segments:
        pacot.settimeout(tempout / 1000.0)

        # Envia REQs até preencher a janela
        while next_seq < base + args.window and next_seq < total_segments:
            time_envios[next_seq] = perf_counter()
            pacot.sendto(f"REQ|seq={next_seq}".encode("utf-8"), srdt)
            next_seq += 1

        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = perf_counter()

            partes = dado.split(b"|", 3)
            if partes[0] == b"DATA":
                num_seq = int(partes[1].split(b"=")[1])
                payload_bin = partes[3]

                if num_seq < base:
                    pacot.sendto(f"ACK|seq={num_seq}".encode("utf-8"), srdt)
                    continue

                if args.loss > 0.0 and random.random() < args.loss:
                    metricas_segmento[num_seq]["simulated_loss"] = 1
                    continue

                if base <= num_seq < base + args.window:
                    pacot.sendto(f"ACK|seq={num_seq}".encode("utf-8"), srdt)

                    if not ack_recebido[num_seq]:
                        ack_recebido[num_seq] = True
                        buffer_partes[num_seq] = payload_bin

                        rtt_amostra = (fim_req - time_envios[num_seq]) * 1000.0
                        
                        # Karn: só atualiza EWMA se o pacote não sofreu retransmissão
                        if metricas_segmento[num_seq]["retransmissions"] == 0:
                            atualizar_rtt_adaptativo(rtt_amostra)

                        m = metricas_segmento[num_seq]
                        m["rtt_ms"] = round(rtt_amostra, 2)
                        m["rtt_estimated_ms"] = round(estimated_rtt, 2) if estimated_rtt is not None else 0.0
                        m["timeout_ms"] = round(tempout, 2)
                        m["size_bytes"] = len(payload_bin)
                        m["throughput_kbps"] = round((len(payload_bin) * 8.0) / rtt_amostra, 2) if rtt_amostra > 0.001 else 0.0
                        m["timestamp"] = datetime.now(timezone.utc).isoformat()

                    while base < total_segments and ack_recebido[base]:
                        base += 1
                        if base % 100 == 0:
                            print(f"\r[*] Progresso SR: {base}/{total_segments} | Timeout: {tempout:.1f}ms", end="", flush=True)

        except (timeout, TimeoutError):
            metricas_segmento[base]["timeout_occurred"] = 1
            metricas_segmento[base]["retransmissions"] += 1
            tempout = min(tempout * 2.0, 5000.0)
            time_envios[base] = perf_counter()
            pacot.sendto(f"REQ|seq={base}".encode("utf-8"), srdt)

print(f"\n[*] Concluído. Validando integridade...")

# FIN
arquivo_final = bytearray()
for p in buffer_partes:
    if p is not None:
        arquivo_final.extend(p)

md5_calculado = hashlib.md5(arquivo_final).hexdigest()
print(f"[*] MD5 Servidor : {checksum_esperado}")
print(f"[*] MD5 Calculado: {md5_calculado}")

pacot.settimeout(2.0)
ok_status = "0"
while True:
    try:
        pacot.sendto(f"FIN|checksum={md5_calculado}".encode("utf-8"), srdt)
        done_raw, _ = pacot.recvfrom(65507)
        if done_raw.startswith(b"DONE"):
            partes_done = done_raw.decode("utf-8", errors="ignore").split("|")
            ok_status = partes_done[1].split("=")[1].strip()
            break
    except (timeout, TimeoutError):
        print("[!] Timeout no FIN, tentando novamente...")

if ok_status == "1":
    print("[+] SUCESSO: Arquivo íntegro validado com sucesso!")
    with open(f"recebido_{args.file}_{args.protocol}_adap.bin", "wb") as f:
        f.write(arquivo_final)
else:
    print("[-] ERRO: Checksum divergente!")

pacot.close()

# CSV
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"metrics_{args.group}_{args.protocol}_adap_w{args.window}_l{int(args.loss*100)}_{timestamp_str}.csv"

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
            "rtt_estimated_ms": m["rtt_estimated_ms"],
            "timeout_ms": m["timeout_ms"],
            "throughput_kbps": m["throughput_kbps"],
            "retransmissions": m["retransmissions"],
            "timeout_occurred": m["timeout_occurred"],
            "window_size": args.window,
            "protocol": f"{args.protocol}_adaptive",
            "simulated_loss": m["simulated_loss"]
        })

print(f"[+] Métricas salvas com sucesso em: {csv_filename}")