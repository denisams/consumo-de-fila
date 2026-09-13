using ConsumoDeFila.Api.Servicos;
using ConsumoDeFila.Compartilhado;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

builder.Services.Configure<OpcoesKafka>(builder.Configuration.GetSection(OpcoesKafka.Secao));
builder.Services.AddSingleton<ProdutorKafka>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseAuthorization();

app.MapControllers();

app.Run();
