using Confluent.Kafka;
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
    public IActionResult ObterStatusFila([FromQuery] string? topico)
    {
        var nomeTopico = topico ?? _opcoes.TopicoPedidos;

        var configuracaoAdmin = new AdminClientConfig { BootstrapServers = _opcoes.EnderecosServidor };
        using var adminClient = new AdminClientBuilder(configuracaoAdmin).Build();

        var metadados = adminClient.GetMetadata(nomeTopico, TimeSpan.FromSeconds(10));
        var topicoMetadados = metadados.Topics.FirstOrDefault(t => t.Topic == nomeTopico);

        if (topicoMetadados is null || topicoMetadados.Partitions.Count == 0)
            return NotFound($"Tópico '{nomeTopico}' não encontrado.");

        var configuracaoConsumidor = new ConsumerConfig
        {
            BootstrapServers = _opcoes.EnderecosServidor,
            GroupId = _opcoes.GrupoConsumidores
        };
        using var consumidor = new ConsumerBuilder<string, string>(configuracaoConsumidor).Build();

        var particoes = topicoMetadados.Partitions
            .Select(particao => new TopicPartition(nomeTopico, particao.PartitionId))
            .ToList();

        var committed = consumidor.Committed(particoes, TimeSpan.FromSeconds(10))
            .ToDictionary(c => c.Partition.Value);

        var status = particoes.Select(tp =>
        {
            var watermarks = consumidor.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10));
            var offsetCommitado = committed[tp.Partition.Value].Offset;
            var offsetCommitadoValor = offsetCommitado.IsSpecial ? watermarks.Low.Value : offsetCommitado.Value;
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
}
