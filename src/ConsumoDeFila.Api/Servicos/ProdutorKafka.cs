using System.Text.Json;
using Confluent.Kafka;
using ConsumoDeFila.Compartilhado;
using Microsoft.Extensions.Options;

namespace ConsumoDeFila.Api.Servicos;

public class ProdutorKafka : IAsyncDisposable
{
    private readonly IProducer<string, string> _produtor;
    private readonly OpcoesKafka _opcoes;

    public ProdutorKafka(IOptions<OpcoesKafka> opcoes)
    {
        _opcoes = opcoes.Value;
        var configuracao = new ProducerConfig { BootstrapServers = _opcoes.EnderecosServidor };
        _produtor = new ProducerBuilder<string, string>(configuracao).Build();
    }

    public async Task<DeliveryResult<string, string>> EnviarPedidoAsync(Pedido pedido, CancellationToken ct)
    {
        var mensagem = new Message<string, string>
        {
            Key = pedido.Id.ToString(),
            Value = JsonSerializer.Serialize(pedido)
        };

        return await _produtor.ProduceAsync(_opcoes.TopicoPedidos, mensagem, ct);
    }

    public ValueTask DisposeAsync()
    {
        _produtor.Flush(TimeSpan.FromSeconds(5));
        _produtor.Dispose();
        return ValueTask.CompletedTask;
    }
}
