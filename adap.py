import argparse
import csv
from datetime import datetime, timezone
import hashlib
import random
from socket import *
from time import perf_counter

# param
parser = argparse.ArgumentParser(description="Cliente RDT-UnB - Entrega 3: GBN e SR com timeout adaptativo")
parser.add_argument("--server", default="137.131.178.229", help="IP do servidor")
parser.add_argument("--port", type=int, default=8080, help="Porta UDP do servidor")
parser.add_argument("--group", required=True, help="Identificador do grupo (ex: grupo11)")
parser.add_argument("--file", default="small", choices=["small", "medium", "large"], help="Arquivo a baixar")
parser.add_argument("--protocol", default="gbn", choices=["gbn", "sr"], help="Protocolo de janela deslizante")
parser.add_argument("--window", type=int, default=4, choices=[4, 8, 16], help="Tamanho da janela (N)")
parser.add_argument("--loss", type=float, default=0.0, choices=[0.0, 0.05, 0.10], help="Taxa de perda simulada")
parser.add_argument("--alpha", type=float, default=0.125, help="Fator de ponderação para RTT estimado (EWMA)")
parser.add_argument("--beta", type=float, default=0.25, help="Fator de ponderação para DevRTT")
parser.add_argument("--timeout-fixo", type=float, default=None, metavar="MS",
                    help="Usa timeout fixo (ms) em vez do adaptativo; a EWMA continua sendo calculada e gravada")

args = parser.parse_args()

srdt = (args.server, args.port)
pacot = socket(AF_INET, SOCK_DGRAM)

# EWMA
ALPHA = args.alpha
BETA = args.beta
FIXO = args.timeout_fixo is not None   # True = timeout fixo (comparação)
PISO_MS = 50.0                          # timeout mínimo
TETO_MS = 5000.0                        # timeout máximo do backoff
MODO = "fixo" if FIXO else "adap"

estimated_rtt = None      # RTT_est (ms)
dev_rtt = None            # DevRTT (ms)
tempout = args.timeout_fixo if FIXO else 1000.0  # timeout em vigor (ms); 1000 até a 1a amostra
erros_estimativa = []     # |RTT_medido - RTT_est anterior| de cada amostra válida

pacot.settimeout(tempout / 1000.0)
session_start = perf_counter()  # tempo total: do HELLO até o DONE

# HELLO
modo_txt = f"timeout fixo={args.timeout_fixo:.0f}ms" if FIXO else f"adaptativo alpha={ALPHA}, beta={BETA}"
print(f"[*] Iniciando {args.protocol.upper()} (Janela={args.window}, Perda={args.loss*100}%, {modo_txt})...")
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
        "timeout_count": 0,
        "simulated_loss": 0,
        "rtt_ms": "",
        "rtt_valid": "",
        "rtt_estimated_ms": 0.0,
        "timeout_ms": 0.0,
        "throughput_kbps": 0.0,
        "timestamp": "",
        "size_bytes": 0
    }


def atualizar_rtt_adaptativo(rtt_amostra):
    """Atualiza RTT estimado e DevRTT (EWMA); no modo adaptativo, também o timeout."""
    global estimated_rtt, dev_rtt, tempout
    if estimated_rtt is None:
        estimated_rtt = rtt_amostra
        dev_rtt = rtt_amostra / 2.0
    else:
        erros_estimativa.append(abs(rtt_amostra - estimated_rtt))
        dev_rtt = (1.0 - BETA) * dev_rtt + BETA * abs(rtt_amostra - estimated_rtt)
        estimated_rtt = (1.0 - ALPHA) * estimated_rtt + ALPHA * rtt_amostra
    recalcular_timeout()


def recalcular_timeout():
    """Timeout = RTT_est + 4*DevRTT (com piso). Chamado também no progresso, para desfazer o backoff."""
    global tempout
    if not FIXO and estimated_rtt is not None:
        tempout = max(estimated_rtt + 4.0 * dev_rtt, PISO_MS)


def aplicar_backoff():
    """No timeout, o adaptativo dobra o prazo (até o teto); o fixo não muda."""
    global tempout
    if not FIXO:
        tempout = min(tempout * 2.0, TETO_MS)


transfer_start = None
transfer_end = None
bytes_recebidos = 0


def registrar(seq, payload, fim_req, envio):
    """Grava as métricas do segmento aceito e desfaz o backoff (houve progresso)."""
    global transfer_end, bytes_recebidos
    m = metricas_segmento[seq]
    rtt_amostra = (fim_req - envio) * 1000.0
    valido = m["retransmissions"] == 0
    # Karn: só amostras sem retransmissão entram na EWMA e no CSV
    if valido:
        atualizar_rtt_adaptativo(rtt_amostra)
    bytes_recebidos += len(payload)
    transfer_end = fim_req
    decorrido = fim_req - transfer_start
    m["rtt_ms"] = round(rtt_amostra, 3) if valido else ""
    m["rtt_valid"] = int(valido)
    m["rtt_estimated_ms"] = round(estimated_rtt, 3) if estimated_rtt is not None else ""
    m["timeout_ms"] = round(tempout, 3)  # prazo em vigor: mostra o backoff nos segmentos retransmitidos
    m["size_bytes"] = len(payload)
    # throughput acumulado: bytes recebidos até aqui / tempo desde o 1o REQ
    m["throughput_kbps"] = round(bytes_recebidos * 8.0 / (decorrido * 1000.0), 3) if decorrido > 0 else 0.0
    m["timestamp"] = datetime.now(timezone.utc).isoformat()
    recalcular_timeout()


def separar_data(dado):
    """b'DATA|seq=N|total=T|payload' -> (N, payload) ou None se inválido."""
    partes = dado.split(b"|", 3)
    if len(partes) < 4 or partes[0] != b"DATA":
        return None
    try:
        num_seq = int(partes[1].split(b"=")[1])
    except (IndexError, ValueError):
        return None
    if not 0 <= num_seq < total_segments:
        return None
    return num_seq, partes[3]


# GBN
if args.protocol == "gbn":
    base = 0
    next_seq = 0
    time_envios = {}

    while base < total_segments:
        pacot.settimeout(tempout / 1000.0)

        # Envia REQs até preencher a janela
        while next_seq < base + args.window and next_seq < total_segments:
            time_envios[next_seq] = perf_counter()
            if transfer_start is None:
                transfer_start = time_envios[next_seq]
            pacot.sendto(f"REQ|seq={next_seq}".encode("utf-8"), srdt)
            next_seq += 1

        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = perf_counter()
            r = separar_data(dado)
            if r is None:
                continue
            num_seq, payload_bin = r

            # Simula perda
            if args.loss > 0.0 and random.random() < args.loss:
                metricas_segmento[num_seq]["simulated_loss"] = 1
                continue

            # No GBN só aceita se for a base esperada em ordem
            if num_seq == base:
                buffer_partes[base] = payload_bin
                pacot.sendto(f"ACK|seq={base}".encode("utf-8"), srdt)  # ACK cumulativo
                registrar(base, payload_bin, fim_req, time_envios[base])
                base += 1
                if base % 100 == 0:
                    print(f"\r[*] Progresso GBN: {base}/{total_segments} | Timeout: {tempout:.1f}ms", end="", flush=True)

        except (timeout, TimeoutError):
            metricas_segmento[base]["timeout_occurred"] = 1
            metricas_segmento[base]["timeout_count"] += 1
            for s in range(base, next_seq):
                metricas_segmento[s]["retransmissions"] += 1
            aplicar_backoff()
            next_seq = base  # reenvia a janela inteira

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
            if transfer_start is None:
                transfer_start = time_envios[next_seq]
            pacot.sendto(f"REQ|seq={next_seq}".encode("utf-8"), srdt)
            next_seq += 1

        try:
            dado, _ = pacot.recvfrom(65507)
            fim_req = perf_counter()
            r = separar_data(dado)
            if r is None:
                continue
            num_seq, payload_bin = r

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
                    registrar(num_seq, payload_bin, fim_req, time_envios[num_seq])

                while base < total_segments and ack_recebido[base]:
                    base += 1
                    if base % 100 == 0:
                        print(f"\r[*] Progresso SR: {base}/{total_segments} | Timeout: {tempout:.1f}ms", end="", flush=True)

        except (timeout, TimeoutError):
            metricas_segmento[base]["timeout_occurred"] = 1
            metricas_segmento[base]["timeout_count"] += 1
            metricas_segmento[base]["retransmissions"] += 1
            aplicar_backoff()
            time_envios[base] = perf_counter()
            pacot.sendto(f"REQ|seq={base}".encode("utf-8"), srdt)

if transfer_end is None:
    transfer_end = perf_counter()

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

session_end = perf_counter()

if ok_status == "1":
    print("[+] SUCESSO: Arquivo íntegro validado com sucesso!")
    with open(f"recebido_{args.file}_{args.protocol}_{MODO}.bin", "wb") as f:
        f.write(arquivo_final)
else:
    print("[-] ERRO: Checksum divergente!")

pacot.close()

# CSV
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
sufixo = f"{args.group}_{args.protocol}_{MODO}_w{args.window}_l{int(round(args.loss*100))}_{timestamp_str}"
csv_filename = f"metrics_{sufixo}.csv"

campos_csv = [
    "segment", "timestamp", "size_bytes", "rtt_ms", "rtt_estimated_ms",
    "timeout_ms", "throughput_kbps", "retransmissions", "timeout_occurred",
    "window_size", "protocol", "simulated_loss",
    "rtt_valid", "timeout_count"
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
            "protocol": f"{args.protocol}_{'fixed' if FIXO else 'adaptive'}",
            "simulated_loss": m["simulated_loss"],
            "rtt_valid": m["rtt_valid"],
            "timeout_count": m["timeout_count"]
        })

print(f"[+] Métricas salvas com sucesso em: {csv_filename}")

# Resumo da execução (tabelas do relatório)
total_time_s = session_end - session_start
transfer_time_s = transfer_end - transfer_start
rtts_validos = [m["rtt_ms"] for m in metricas_segmento.values() if m["rtt_valid"] == 1]
timeouts_ms = [m["timeout_ms"] for m in metricas_segmento.values() if m["timestamp"]]
resumo = {
    "protocol": args.protocol,
    "timeout_mode": "fixed" if FIXO else "adaptive",
    "alpha": ALPHA,
    "beta": BETA,
    "fixed_timeout_ms": args.timeout_fixo if FIXO else "",
    "window_size": args.window,
    "loss": args.loss,
    "file": args.file,
    "md5_ok": int(ok_status == "1"),
    "total_time_s": round(total_time_s, 6),
    "transfer_time_s": round(transfer_time_s, 6),
    "average_throughput_kbps": round(len(arquivo_final) * 8 / total_time_s / 1000, 3),
    "transfer_throughput_kbps": round(len(arquivo_final) * 8 / transfer_time_s / 1000, 3),
    "total_retransmissions": sum(m["retransmissions"] for m in metricas_segmento.values()),
    "total_timeouts": sum(m["timeout_count"] for m in metricas_segmento.values()),
    "rtt_mean_ms": round(sum(rtts_validos) / len(rtts_validos), 3) if rtts_validos else "",
    "rtt_estimation_mae_ms": round(sum(erros_estimativa) / len(erros_estimativa), 3) if erros_estimativa else "",
    "timeout_mean_ms": round(sum(timeouts_ms) / len(timeouts_ms), 3) if timeouts_ms else "",
}
summary_filename = f"summary_{sufixo}.csv"
with open(summary_filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=resumo.keys())
    writer.writeheader()
    writer.writerow(resumo)
print(f"[+] Resumo da execução: {summary_filename}")
print(f"[*] Tempo total: {resumo['total_time_s']} s | Throughput médio: {resumo['average_throughput_kbps']} kbps | "
      f"Retransmissões: {resumo['total_retransmissions']} | Timeouts: {resumo['total_timeouts']}")
