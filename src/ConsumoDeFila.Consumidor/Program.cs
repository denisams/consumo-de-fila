using ConsumoDeFila.Compartilhado;
using ConsumoDeFila.Consumidor;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<OpcoesKafka>(builder.Configuration.GetSection(OpcoesKafka.Secao));
builder.Services.AddHostedService<ProcessadorPedidosService>();

var host = builder.Build();
host.Run();
