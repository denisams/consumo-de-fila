# Consumo de Fila

Exemplo simples de uso do **Apache Kafka** com .NET 9, simulando um sistema de
processamento de pedidos: uma API publica pedidos numa fila, um worker consome
e processa, falhas vão para uma fila de *dead-letter* e um endpoint expõe o
status/atraso da fila para monitoramento.

## Arquitetura

```
ConsumoDeFila.Api          -> API Web (produtor): recebe pedidos via HTTP e publica no tópico "pedidos"
ConsumoDeFila.Consumidor   -> Worker Service (consumidor): consome "pedidos", processa e
                               encaminha falhas para o tópico "pedidos-dlq"
ConsumoDeFila.Compartilhado -> Modelos e configurações compartilhadas entre API e Consumidor
```

Fluxo:

1. `POST /pedidos` na API cria um `Pedido` e publica no tópico `pedidos`.
2. O `ConsumoDeFila.Consumidor` está inscrito no tópico `pedidos` (grupo de
   consumidores `processador-pedidos`), processa cada pedido e comita o offset.
3. Caso o processamento falhe (simulado, ~10% das vezes), o pedido é
   republicado no tópico `pedidos-dlq` (dead-letter queue) para reprocessamento
   ou análise posterior.
4. `GET /monitoramento/fila` na API mostra, por partição, o offset mais
   recente, o offset já commitado pelo grupo consumidor e quantas mensagens
   estão pendentes (lag).
5. O **Redpanda Console** oferece uma interface visual para navegar nos
   tópicos, ver mensagens, partições e grupos de consumidores.

## Pré-requisitos

- [.NET SDK 9](https://dotnet.microsoft.com/download)
- [Docker](https://www.docker.com/) com Docker Compose

## Como executar

### 1. Subir o Kafka

```powershell
docker compose up -d
```

Isso sobe:

- **Kafka** (modo KRaft, sem Zookeeper) em `localhost:9092`
- **kafka-init**: cria os tópicos `pedidos` (3 partições) e `pedidos-dlq` (1 partição)
- **Redpanda Console** em [http://localhost:8081](http://localhost:8081) para monitorar tópicos, mensagens e consumer groups

### 2. Rodar o Consumidor (Worker)

```powershell
dotnet run --project src/ConsumoDeFila.Consumidor
```

### 3. Rodar a API (Produtor)

```powershell
dotnet run --project src/ConsumoDeFila.Api
```

Por padrão a API sobe em `http://localhost:5220` e abre automaticamente o
navegador na página do **Swagger UI** (`/swagger`), onde dá para testar os
endpoints `POST /pedidos` e `GET /monitoramento/fila` diretamente.

### 4. Publicar um pedido na fila

```powershell
curl -X POST http://localhost:5000/pedidos `
  -H "Content-Type: application/json" `
  -d '{"cliente":"Maria","produto":"Teclado Mecanico","quantidade":1,"valorTotal":350.00}'
```

Acompanhe no log do Consumidor o processamento (e, ocasionalmente, o envio
para a fila de falhas).

### 5. Monitorar a fila

Via API:

```powershell
curl http://localhost:5000/monitoramento/fila
```

Retorna o total de mensagens pendentes e o detalhe por partição (offset
inicial, final, commitado e mensagens pendentes/lag).

Via interface visual: abra [http://localhost:8081](http://localhost:8081) e
explore os tópicos `pedidos` e `pedidos-dlq`, suas mensagens e o consumer
group `processador-pedidos`.

## Configuração

As configurações de conexão com o Kafka ficam em `appsettings.json` de cada
projeto, na seção `Kafka`:

```json
{
  "Kafka": {
    "EnderecosServidor": "localhost:9092",
    "TopicoPedidos": "pedidos",
    "TopicoPedidosFalhos": "pedidos-dlq",
    "GrupoConsumidores": "processador-pedidos"
  }
}
```

## Encerrando

```powershell
docker compose down -v
```
