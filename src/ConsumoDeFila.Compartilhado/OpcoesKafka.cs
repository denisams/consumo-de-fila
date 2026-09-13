namespace ConsumoDeFila.Compartilhado;

public class OpcoesKafka
{
    public const string Secao = "Kafka";

    public string EnderecosServidor { get; set; } = "localhost:9092";
    public string TopicoPedidos { get; set; } = "pedidos";
    public string TopicoPedidosFalhos { get; set; } = "pedidos-dlq";
    public string GrupoConsumidores { get; set; } = "processador-pedidos";
}
