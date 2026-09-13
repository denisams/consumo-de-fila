using ConsumoDeFila.Api.Servicos;
using ConsumoDeFila.Compartilhado;
using Microsoft.AspNetCore.Mvc;

namespace ConsumoDeFila.Api.Controllers;

[ApiController]
[Route("pedidos")]
public class PedidosController : ControllerBase
{
    private readonly ProdutorKafka _produtor;
    private readonly ILogger<PedidosController> _logger;

    public PedidosController(ProdutorKafka produtor, ILogger<PedidosController> logger)
    {
        _produtor = produtor;
        _logger = logger;
    }

    [HttpPost]
    public async Task<IActionResult> Criar([FromBody] CriarPedidoRequisicao requisicao, CancellationToken ct)
    {
        var pedido = new Pedido
        {
            Cliente = requisicao.Cliente,
            Produto = requisicao.Produto,
            Quantidade = requisicao.Quantidade,
            ValorTotal = requisicao.ValorTotal
        };

        var resultado = await _produtor.EnviarPedidoAsync(pedido, ct);

        _logger.LogInformation(
            "Pedido {PedidoId} publicado na partição {Particao}, offset {Offset}",
            pedido.Id, resultado.Partition.Value, resultado.Offset.Value);

        return Accepted(new { pedido.Id });
    }
}

public record CriarPedidoRequisicao(string Cliente, string Produto, int Quantidade, decimal ValorTotal);
