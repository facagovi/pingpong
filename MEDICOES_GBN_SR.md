# Medições de SAW, GBN e SR

Esta branch corrige a temporização e as métricas dos três protocolos em `gbnsr.py`.

## Timeout e retransmissões

- SAW: o timeout de 1 s é contado desde o último REQ do segmento atual.
  Um DATA descartado pela simulação não provoca reenvio imediato: o cliente
  espera esse prazo vencer. Cada novo REQ incrementa `retransmissions`.
- GBN: o timeout de 1 s é contado desde o último envio do REQ da base.
  Quando a base expira, o cliente retransmite a base e todos os segmentos
  seguintes ainda pendentes na janela. `timeout_count` cresce somente para a
  base que expirou; `retransmissions` cresce para cada REQ reenviado.
- SR: cada REQ não recebido tem seu próprio prazo de 1 s desde o último envio.
  Ao expirar, somente aquele REQ é reenviado. Receber outros segmentos ou
  duplicatas não prorroga esse prazo.

## CSV por segmento

O arquivo `metrics_*.csv` conserva as colunas anteriores e acrescenta, para
SAW, GBN e SR:

- `rtt_ms`: tempo do envio ao recebimento apenas quando houve uma única
  tentativa. Fica vazio após retransmissão, pois a resposta pode corresponder
  a qualquer uma das tentativas.
- `rtt_valid`: 1 quando `rtt_ms` é válido; 0 quando houve retransmissão.
- `segment_elapsed_ms`: tempo entre o primeiro REQ do segmento e o DATA aceito,
  incluindo espera e retransmissões.
- `throughput_kbps`: bits úteis do segmento divididos por
  `segment_elapsed_ms`. É uma taxa por segmento, não a taxa média do arquivo.
- `timeout_count`: número de expirações de prazo atribuídas ao segmento.
  `timeout_occurred` continua sendo apenas um indicador binário.
- `retransmissions`: número de REQs adicionais enviados para o segmento.
- `simulated_loss`: continua sendo um indicador binário. Um segmento pode ter
  vários descartes simulados e, portanto, mais retransmissões do que o valor
  desse campo sugere.

## Resumo da execução

Cada execução gera também `summary_*.csv`, com uma linha:

- `total_time_s`: do início do HELLO até o recebimento de DONE;
- `average_throughput_kbps`: bits úteis recebidos divididos por `total_time_s`;
- `transfer_time_s`: do primeiro REQ até o último DATA aceito;
- `transfer_throughput_kbps`: bits úteis divididos por `transfer_time_s`;
- `total_retransmissions` e `total_timeouts`: somas dos respectivos campos do
  CSV por segmento.

Use `total_time_s`, `average_throughput_kbps` e `total_retransmissions` da
**mesma execução** para a tabela comparativa. Os CSVs antigos foram produzidos
com outra lógica de timeout e outra definição de throughput por segmento;
seus valores não devem ser misturados aos novos resultados.
