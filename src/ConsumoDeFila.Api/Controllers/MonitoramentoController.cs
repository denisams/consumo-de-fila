using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ConsumoDeFila.Compartilhado;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace ConsumoDeFila.Api.Controllers;

[ApiController]
[Route("monitoramento")]
public class MonitoramentoController : ControllerBase
{
    private readonly OpcoesKafka _opcoes;

    public MonitoramentoController(IOptions<OpcoesKafka> opcoes)
    {
        _opcoes = opcoes.Value;
    }

    [HttpGet("fila")]
    public async Task<IActionResult> ObterStatusFila([FromQuery] string? topico)
    {
        var nomeTopico = topico ?? _opcoes.TopicoPedidos;

        var configuracaoAdmin = new AdminClientConfig { BootstrapServers = _opcoes.EnderecosServidor };
        using var adminClient = new AdminClientBuilder(configuracaoAdmin).Build();

        var metadados = adminClient.GetMetadata(nomeTopico, TimeSpan.FromSeconds(10));
        var topicoMetadados = metadados.Topics.FirstOrDefault(t => t.Topic == nomeTopico);

        if (topicoMetadados is null || topicoMetadados.Partitions.Count == 0)
            return NotFound($"Tópico '{nomeTopico}' não encontrado.");

        var particoes = topicoMetadados.Partitions
            .Select(particao => new TopicPartition(nomeTopico, particao.PartitionId))
            .ToList();

        // Consulta os offsets commitados via API administrativa, em vez de um Consumer
        // dedicado: evita erros transitórios de "not coordinator" logo após o broker
        // subir ou quando o grupo ainda não commitou nada.
        var offsetsCommitados = await ObterOffsetsCommitadosAsync(adminClient, _opcoes.GrupoConsumidores, particoes);

        // Watermarks (offset inicial/final de cada partição) exigem um cliente
        // consumidor, mas usamos um grupo descartável só para essa consulta, sem
        // afetar o grupo de consumidores real.
        var configuracaoConsumidor = new ConsumerConfig
        {
            BootstrapServers = _opcoes.EnderecosServidor,
            GroupId = $"monitoramento-{Guid.NewGuid()}"
        };
        using var consumidor = new ConsumerBuilder<string, string>(configuracaoConsumidor).Build();

        var status = particoes.Select(tp =>
        {
            var watermarks = consumidor.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10));
            var offsetCommitadoValor = offsetsCommitados.TryGetValue(tp.Partition.Value, out var offset)
                ? offset.Value
                : watermarks.Low.Value;
            var mensagensPendentes = watermarks.High.Value - offsetCommitadoValor;

            return new
            {
                Particao = tp.Partition.Value,
                OffsetInicial = watermarks.Low.Value,
                OffsetFinal = watermarks.High.Value,
                OffsetCommitado = offsetCommitadoValor,
                MensagensPendentes = mensagensPendentes
            };
        }).ToList();

        return Ok(new
        {
            Topico = nomeTopico,
            GrupoConsumidores = _opcoes.GrupoConsumidores,
            TotalPendente = status.Sum(s => s.MensagensPendentes),
            Particoes = status
        });
    }

    private static async Task<Dictionary<int, Offset>> ObterOffsetsCommitadosAsync(
        IAdminClient adminClient, string grupo, List<TopicPartition> particoes)
    {
        var resultado = new Dictionary<int, Offset>();

        try
        {
            var grupos = await adminClient.ListConsumerGroupOffsetsAsync(
                new[] { new ConsumerGroupTopicPartitions(grupo, particoes) });

            foreach (var offsetParticao in grupos.SelectMany(g => g.Partitions))
            {
                if (!offsetParticao.Error.IsError && !offsetParticao.Offset.IsSpecial)
                    resultado[offsetParticao.Partition.Value] = offsetParticao.Offset;
            }
        }
        catch (KafkaException)
        {
            // grupo ainda sem offsets commitados (ex.: consumidor nunca rodou) — fica vazio
        }

        return resultado;
    }
}
