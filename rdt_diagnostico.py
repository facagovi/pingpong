#!/usr/bin/env python3
"""
RDT-UnB — Diagnóstico do Servidor
Uso: python3 rdt_diagnostico.py --server 137.131.178.229 --group grupo01
"""

import socket
import time
import argparse
import datetime

SERVER  = "137.131.178.229"
PORT    = 8080
TIMEOUT = 5.0

def criar_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(TIMEOUT)
    return s

def teste_ping(s, srv):
    print("\n[1] PING / PONG")
    try:
        t0 = time.time()
        s.sendto(b"PING", srv)
        dados, _ = s.recvfrom(65507)
        rtt = (time.time() - t0) * 1000
        resposta = dados.decode("utf-8")
        ts = float(resposta.split("|")[1].split("=")[1])
        hora_srv = datetime.datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S")
        print(f"  OK  PONG recebido")
        print(f"    RTT:              {rtt:.1f} ms")
        print(f"    Hora no servidor: {hora_srv}")
        return True
    except socket.timeout:
        print("  ERRO  Timeout — servidor nao respondeu ao PING")
        return False
    except Exception as e:
        print(f"  ERRO  {e}")
        return False

def teste_hello(s, srv, grupo):
    print("\n[2] HELLO / OK")
    pkt = f"HELLO|grupo={grupo}|segment_size=512|file=small".encode()
    try:
        s.sendto(pkt, srv)
        dados, _ = s.recvfrom(65507)
        resposta = dados.decode("utf-8")
        campos = {}
        for parte in resposta.split("|")[1:]:
            if "=" in parte:
                k, v = parte.split("=", 1)
                campos[k] = v
        if resposta.startswith("OK"):
            print(f"  OK  OK recebido")
            print(f"    Arquivo:          small ({int(campos['file_size'])//1024} KB)")
            print(f"    Checksum MD5:     {campos['checksum']}")
            print(f"    Total segmentos:  {campos['total_segments']}")
            print(f"    Tamanho segmento: {campos['segment_size']} bytes")
            print(f"    Seed:             {campos['seed']}")
            return True
        else:
            print(f"  ERRO  Resposta inesperada: {resposta[:80]}")
            return False
    except socket.timeout:
        print("  ERRO  Timeout — servidor nao respondeu ao HELLO")
        return False
    except Exception as e:
        print(f"  ERRO  {e}")
        return False

def teste_req(s, srv, grupo):
    print("\n[3] REQ / DATA / ACK (sequencia de 2 segmentos)")

    # Handshake primeiro
    pkt = f"HELLO|grupo={grupo}|segment_size=512|file=small".encode()
    try:
        s.sendto(pkt, srv)
        dados, _ = s.recvfrom(65507)
        if not dados.decode("utf-8").startswith("OK"):
            print("  ERRO  Handshake falhou — nao foi possivel testar REQ")
            return False
    except socket.timeout:
        print("  ERRO  Timeout no handshake — nao foi possivel testar REQ")
        return False

    # Solicita segmento 0
    try:
        t0 = time.time()
        s.sendto(b"REQ|seq=0", srv)
        dados, _ = s.recvfrom(65507)
        rtt = (time.time() - t0) * 1000

        # Parseia cabecalho: DATA|seq=N|total=M|<payload binario>
        # O cabecalho e texto puro; o payload e binario e pode conter '|'
        # Estrategia: decodifica so ate o 4o '|' (limite do cabecalho)
        def parse_data_packet(raw):
            """Retorna (tipo, seq_int, total_int, payload_bytes) ou lanca ValueError."""
            # Procura os 3 primeiros '|' apenas nos primeiros 64 bytes (cabecalho e curto)
            header_region = raw[:64]
            count = 0
            cut = -1
            for i, b in enumerate(header_region):
                if b == ord("|"):
                    count += 1
                    if count == 3:
                        cut = i + 1
                        break
            if cut == -1:
                raise ValueError("subsection not found")
            header = raw[:cut-1].decode("utf-8")
            partes = header.split("|")
            tipo   = partes[0]
            seq_n  = int(partes[1].split("=")[1])
            total  = int(partes[2].split("=")[1])
            payload = raw[cut:]
            return tipo, seq_n, total, payload

        tipo, seq_n, total, payload = parse_data_packet(dados)

        if tipo == "DATA" and seq_n == 0:
            print(f"  OK  DATA seq=0 recebido")
            print(f"    Payload:    {total} bytes  (recebido: {len(payload)} bytes)")
            print(f"    RTT:        {rtt:.1f} ms")
            print(f"    8 bytes:    {' '.join(f'{b:02x}' for b in payload[:8])}")
        else:
            print(f"  ERRO  Resposta inesperada: {dados[:60]}")
            return False

        # Protocolo pull-based: nao envia ACK separado.
        # O proximo REQ ja funciona como confirmacao implicita.

        # Solicita segmento 1 para confirmar sequencia
        t0 = time.time()
        s.sendto(b"REQ|seq=1", srv)
        dados2, _ = s.recvfrom(65507)
        rtt2 = (time.time() - t0) * 1000

        _, seq2, _, _ = parse_data_packet(dados2)

        if seq2 == 1:
            print(f"  OK  DATA seq=1 recebido  (RTT: {rtt2:.1f} ms)")
            print(f"\n  Servidor respondendo corretamente a sequencia REQ -> DATA -> ACK -> REQ")
        else:
            print(f"  AVISO  Esperava seq=1, recebeu seq={seq2}")

        return True

    except socket.timeout:
        print("  ERRO  Timeout — servidor nao respondeu ao REQ")
        print("    Possivel causa: servidor parado ou sessao expirada")
        return False
    except Exception as e:
        print(f"  ERRO  {e}")
        return False

def main():
    p = argparse.ArgumentParser(description="RDT-UnB — Diagnostico do Servidor")
    p.add_argument("--server", default=SERVER,      help=f"Endereco do servidor (padrao: {SERVER})")
    p.add_argument("--port",   type=int, default=PORT, help=f"Porta UDP (padrao: {PORT})")
    p.add_argument("--group",  default="diag",      help="ID do grupo (padrao: diag)")
    args = p.parse_args()

    srv = (args.server, args.port)

    print("=" * 52)
    print(f"  RDT-UnB Diagnostico")
    print(f"  Servidor: {args.server}:{args.port}")
    print(f"  Grupo:    {args.group}")
    print("=" * 52)

    # Testes 1 e 2 no mesmo socket
    s = criar_socket()
    ok1 = teste_ping(s, srv)
    ok2 = teste_hello(s, srv, args.group)
    s.close()

    # Teste 3 em socket novo (sessao limpa apos o HELLO anterior)
    s2 = criar_socket()
    ok3 = teste_req(s2, srv, args.group)
    s2.close()

    # Resumo
    print("\n" + "=" * 52)
    print(f"  RESUMO")
    print(f"  [1] PING / PONG :  {'OK' if ok1 else 'FALHOU'}")
    print(f"  [2] HELLO / OK  :  {'OK' if ok2 else 'FALHOU'}")
    print(f"  [3] REQ / DATA  :  {'OK' if ok3 else 'FALHOU'}")
    print()
    if ok1 and ok2 and ok3:
        print("  Servidor operacional.")
    elif ok1 and not ok2:
        print("  Servidor ativo mas rejeitando HELLO.")
        print("  Verifique o formato do pacote e o ID do grupo.")
    elif ok1 and ok2 and not ok3:
        print("  Handshake OK mas REQ sem resposta.")
        print("  Possivel bug no loop de sessao do servidor.")
    elif not ok1:
        print("  Servidor inacessivel. Verifique:")
        print("    - Se o servidor esta rodando: ps aux | grep rdt_server")
        print("    - Se a porta 8080/UDP esta aberta no firewall")
        print("    - Se o endereco esta correto")
    print("=" * 52)

if __name__ == "__main__":
    main()