using System.Text.Json;
using Confluent.Kafka;
using ConsumoDeFila.Compartilhado;
using Microsoft.Extensions.Options;

namespace ConsumoDeFila.Consumidor;

public class ProcessadorPedidosService : BackgroundService
{
    private readonly OpcoesKafka _opcoes;
    private readonly ILogger<ProcessadorPedidosService> _logger;
    private readonly Random _aleatorio = new();

    public ProcessadorPedidosService(IOptions<OpcoesKafka> opcoes, ILogger<ProcessadorPedidosService> logger)
    {
        _opcoes = opcoes.Value;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consumer do Confluent.Kafka é bloqueante, por isso roda numa thread dedicada
        return Task.Run(() => ConsumirMensagens(stoppingToken), stoppingToken);
    }

    private void ConsumirMensagens(CancellationToken ct)
    {
        var configuracaoConsumidor = new ConsumerConfig
        {
            BootstrapServers = _opcoes.EnderecosServidor,
            GroupId = _opcoes.GrupoConsumidores,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var configuracaoProdutorFalhas = new ProducerConfig { BootstrapServers = _opcoes.EnderecosServidor };

        using var consumidor = new ConsumerBuilder<string, string>(configuracaoConsumidor).Build();
        using var produtorFalhas = new ProducerBuilder<string, string>(configuracaoProdutorFalhas).Build();

        consumidor.Subscribe(_opcoes.TopicoPedidos);
        _logger.LogInformation(
            "Consumidor inscrito no tópico {Topico}, grupo {Grupo}",
            _opcoes.TopicoPedidos, _opcoes.GrupoConsumidores);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string>? resultado;
                try
                {
                    resultado = consumidor.Consume(ct);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Erro ao consumir mensagem do tópico {Topico}", _opcoes.TopicoPedidos);
                    continue;
                }

                if (resultado?.Message is null)
                    continue;

                var pedido = JsonSerializer.Deserialize<Pedido>(resultado.Message.Value);
                if (pedido is null)
                {
                    _logger.LogWarning("Mensagem inválida descartada no offset {Offset}", resultado.Offset.Value);
                    consumidor.Commit(resultado);
                    continue;
                }

                if (ProcessarPedido(pedido))
                {
                    _logger.LogInformation(
                        "Pedido {PedidoId} processado com sucesso (partição {Particao}, offset {Offset})",
                        pedido.Id, resultado.Partition.Value, resultado.Offset.Value);
                    consumidor.Commit(resultado);
                }
                else
                {
                    pedido.TentativasProcessamento++;
                    _logger.LogWarning(
                        "Falha ao processar pedido {PedidoId}, enviando para {TopicoFalhas} (tentativa {Tentativa})",
                        pedido.Id, _opcoes.TopicoPedidosFalhos, pedido.TentativasProcessamento);

                    produtorFalhas.Produce(_opcoes.TopicoPedidosFalhos, new Message<string, string>
                    {
                        Key = pedido.Id.ToString(),
                        Value = JsonSerializer.Serialize(pedido)
                    });
                    produtorFalhas.Flush(TimeSpan.FromSeconds(5));
                    consumidor.Commit(resultado);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // encerramento solicitado, nada a fazer
        }
        finally
        {
            consumidor.Close();
        }
    }

    private bool ProcessarPedido(Pedido pedido)
    {
        Thread.Sleep(300);

        // simula falha ocasional (~10%) para exercitar o fluxo de dead-letter
        return _aleatorio.Next(0, 10) != 0;
    }
}
