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
TIMEOUT_SECONDS = 1.0
pacot.settimeout(TIMEOUT_SECONDS)
session_start = time()

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
        "rtt_valid": "",
        "segment_elapsed_ms": "",
        "timeout_count": "",
        "throughput_kbps": 0.0,
        "timestamp": "",
        "size_bytes": 0
    }


def registrar_segmento(seq, payload, instante, ultimo_envio, primeiro_envio):
    """Registra RTT apenas quando não há ambiguidade por retransmissão."""
    m = metricas_segmento[seq]
    duracao_ms = (instante - primeiro_envio[seq]) * 1000
    m["segment_elapsed_ms"] = round(duracao_ms, 3)
    m["rtt_valid"] = int(m["retransmissions"] == 0)
    m["rtt_ms"] = round((instante - ultimo_envio[seq]) * 1000, 3) if m["rtt_valid"] else ""
    m["size_bytes"] = len(payload)
    # Taxa efetiva deste segmento: inclui toda a espera e suas retransmissões.
    m["throughput_kbps"] = round(len(payload) * 8 / duracao_ms, 3) if duracao_ms > 0 else 0.0
    m["timestamp"] = datetime.now(timezone.utc).isoformat()


transfer_start = None
transfer_end = None

# STOP AND WAIT
if args.protocol == "saw":
    seq_atual = 0
    time_envios = {}
    primeiro_envio = {}
    while seq_atual < total_segments:
        time_envios[seq_atual] = time()
        primeiro_envio[seq_atual] = time_envios[seq_atual]
        if transfer_start is None:
            transfer_start = time_envios[seq_atual]
        metricas_segmento[seq_atual]["timeout_count"] = 0
        pacot.sendto(f"REQ|seq={seq_atual}".encode(), srdt)

        while True:
            espera = time_envios[seq_atual] + TIMEOUT_SECONDS - time()
            if espera <= 0:
                m = metricas_segmento[seq_atual]
                m["timeout_occurred"] = 1
                m["timeout_count"] += 1
                m["retransmissions"] += 1
                time_envios[seq_atual] = time()
                pacot.sendto(f"REQ|seq={seq_atual}".encode(), srdt)
                continue

            pacot.settimeout(espera)
            try:
                dado, _ = pacot.recvfrom(65507)
            except (timeout, TimeoutError):
                # A próxima iteração confere o prazo do REQ enviado.
                continue

            fim_req = time()
            # Separa os três primeiros pipes sem decodificar o payload binário.
            partes = dado.split(b"|", 3)
            if partes[0] != b"DATA":
                continue
            num_seq = int(partes[1].split(b"=")[1])
            if num_seq != seq_atual:
                continue
            payload_bin = partes[3]

            if args.loss > 0.0 and random.random() < args.loss:
                metricas_segmento[seq_atual]["simulated_loss"] = 1
                # O DATA foi ignorado; o REQ só será reenviado no timeout.
                continue

            buffer_partes[seq_atual] = payload_bin
            pacot.sendto(f"ACK|seq={seq_atual}".encode(), srdt)  # confirma o segmento
            registrar_segmento(seq_atual, payload_bin, fim_req,
                               time_envios, primeiro_envio)
            del time_envios[seq_atual]
            seq_atual += 1
            if seq_atual == total_segments:
                transfer_end = fim_req
            break


# GO BACK N
elif args.protocol == "gbn":
    base = 0
    next_seq = 0
    time_envios = {}  # Último REQ enviado para cada segmento pendente.
    primeiro_envio = {}

    while base < total_segments:
        # Preenche a janela; o prazo de timeout da base nasce no envio do REQ.
        while next_seq < base + args.window and next_seq < total_segments:
            time_envios[next_seq] = time()
            if transfer_start is None:
                transfer_start = time_envios[next_seq]
            primeiro_envio.setdefault(next_seq, time_envios[next_seq])
            metricas_segmento[next_seq]["timeout_count"] = 0
            pacot.sendto(f"REQ|seq={next_seq}".encode(), srdt)
            next_seq += 1

        espera = time_envios[base] + TIMEOUT_SECONDS - time()
        if espera <= 0:
            metricas_segmento[base]["timeout_occurred"] = 1
            metricas_segmento[base]["timeout_count"] += 1
            # GBN retransmite a base e todos os REQs seguintes ainda pendentes.
            for seq in range(base, next_seq):
                metricas_segmento[seq]["retransmissions"] += 1
                time_envios[seq] = time()
                pacot.sendto(f"REQ|seq={seq}".encode(), srdt)
            continue

        pacot.settimeout(espera)
        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = time()
            partes = dado.split(b"|", 3)
            if partes[0] == b"DATA":
                num_seq = int(partes[1].split(b"=")[1])
                payload_bin = partes[3]

                if not 0 <= num_seq < total_segments:
                    continue

                # Simula perda
                if args.loss > 0.0 and random.random() < args.loss:
                    metricas_segmento[num_seq]["simulated_loss"] = 1
                    continue

                # No GBN só aceita se for exatamente a base esperada em ordem
                if num_seq == base:
                    buffer_partes[base] = payload_bin
                    pacot.sendto(f"ACK|seq={base}".encode(), srdt)  # ACK cumulativo da base
                    registrar_segmento(base, payload_bin, fim_req, time_envios, primeiro_envio)
                    del time_envios[base]
                    base += 1
                    if base == total_segments:
                        transfer_end = fim_req

        except (timeout, TimeoutError):
            # O prazo é verificado no início da próxima iteração.
            pass


# SELECTIVE REPEAT
elif args.protocol == "sr":
    base = 0
    next_seq = 0
    time_envios = {}  # Um prazo independente para cada REQ não recebido.
    primeiro_envio = {}
    req_rcv = [False] * total_segments

    while base < total_segments:
        # Envia REQs até preencher a janela.
        while next_seq < base + args.window and next_seq < total_segments:
            time_envios[next_seq] = time()
            if transfer_start is None:
                transfer_start = time_envios[next_seq]
            primeiro_envio.setdefault(next_seq, time_envios[next_seq])
            metricas_segmento[next_seq]["timeout_count"] = 0
            pacot.sendto(f"REQ|seq={next_seq}".encode(), srdt)
            next_seq += 1

        agora = time()
        expirados = [seq for seq, enviado in time_envios.items()
                     if enviado + TIMEOUT_SECONDS <= agora]
        if expirados:
            # Cada REQ expirado é retransmitido individualmente; segmentos já
            # recebidos saem de time_envios e não entram nesta lista.
            for seq in expirados:
                m = metricas_segmento[seq]
                m["timeout_occurred"] = 1
                m["timeout_count"] += 1
                m["retransmissions"] += 1
                time_envios[seq] = time()
                pacot.sendto(f"REQ|seq={seq}".encode(), srdt)
            continue

        espera = min(enviado + TIMEOUT_SECONDS for enviado in time_envios.values()) - agora
        pacot.settimeout(max(espera, 0.000001))
        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = time()
            partes = dado.split(b"|", 3)
            if partes[0] == b"DATA":
                num_seq = int(partes[1].split(b"=")[1])
                payload_bin = partes[3]

                if not 0 <= num_seq < total_segments:
                    continue

                # Confirma individualmente com ACK mesmo se for segmento repetido antigo
                if num_seq < base:
                    pacot.sendto(f"ACK|seq={num_seq}".encode(), srdt)
                    continue

                if not base <= num_seq < base + args.window:
                    continue

                if req_rcv[num_seq]:
                    pacot.sendto(f"ACK|seq={num_seq}".encode(), srdt)
                    continue

                # Simula perda
                if args.loss > 0.0 and random.random() < args.loss:
                    metricas_segmento[num_seq]["simulated_loss"] = 1
                    continue

                pacot.sendto(f"ACK|seq={num_seq}".encode(), srdt)
                registrar_segmento(num_seq, payload_bin, fim_req, time_envios, primeiro_envio)
                req_rcv[num_seq] = True
                buffer_partes[num_seq] = payload_bin
                del time_envios[num_seq]

                # Desliza a base enquanto houver segmentos consecutivos prontos.
                while base < total_segments and req_rcv[base]:
                    base += 1
                if base == total_segments:
                    transfer_end = fim_req

        except (timeout, TimeoutError):
            # O prazo de cada segmento é verificado na próxima iteração.
            pass

if transfer_end is None:
    transfer_end = time()

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
pacot.settimeout(TIMEOUT_SECONDS)
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

session_end = time()

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
campos_csv += ["rtt_valid", "segment_elapsed_ms", "timeout_count"]

with open(csv_filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=campos_csv)
    writer.writeheader()
    for seq in range(total_segments):
        m = metricas_segmento[seq]
        linha = {
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
        }
        linha.update({
            "rtt_valid": m["rtt_valid"],
            "segment_elapsed_ms": m["segment_elapsed_ms"],
            "timeout_count": m["timeout_count"]
        })
        writer.writerow(linha)

print(f"[+] Métricas salvas com sucesso em: {csv_filename}")

if args.protocol in ("saw", "gbn", "sr"):
    # Tempo total: HELLO até DONE. A taxa média usa o mesmo intervalo.
    # O intervalo de transferência (primeiro REQ ao último DATA) é separado.
    total_time_s = round(session_end - session_start, 9)
    transfer_time_s = round(transfer_end - transfer_start, 9)
    resumo = {
        "protocol": args.protocol,
        "window_size": args.window,
        "loss": args.loss,
        "file": args.file,
        "total_time_s": total_time_s,
        "transfer_time_s": transfer_time_s,
        "average_throughput_kbps": round(len(arquivo_final) * 8 / total_time_s / 1000, 3),
        "transfer_throughput_kbps": round(len(arquivo_final) * 8 / transfer_time_s / 1000, 3),
        "total_retransmissions": sum(m["retransmissions"] for m in metricas_segmento.values()),
        "total_timeouts": sum(m["timeout_count"] for m in metricas_segmento.values())
    }
    summary_filename = f"summary_{args.group}_{args.protocol}_{timestamp_str}.csv"
    with open(summary_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=resumo.keys())
        writer.writeheader()
        writer.writerow(resumo)
    print(f"[+] Resumo da execução: {summary_filename}")
    print(f"[*] Tempo total: {resumo['total_time_s']} s | "
          f"Throughput médio: {resumo['average_throughput_kbps']} kbps | "
          f"Retransmissões: {resumo['total_retransmissions']}")
