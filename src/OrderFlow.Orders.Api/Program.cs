using Microsoft.EntityFrameworkCore;
using OrderFlow.Contracts;
using OrderFlow.Orders.Api.Data;
using OrderFlow.Orders.Api.Domain;
using OrderFlow.Orders.Api.Endpoints;
using OrderFlow.Orders.Api.Messaging;
using System.Text.Json.Serialization;


var builder = WebApplication.CreateBuilder(args);

// ----- Banco de dados (Supabase / PostgreSQL) -----
builder.Services.AddDbContext<AppDbContext>(opt =>
    opt.UseNpgsql(builder.Configuration.GetConnectionString("Supabase")));

// ----- RabbitMQ -----
var rabbit = builder.Configuration.GetSection("RabbitMq").Get<RabbitMqSettings>() ?? new RabbitMqSettings();
builder.Services.AddSingleton(rabbit);
builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddHostedService<PagamentoProcessadoConsumer>();
builder.Services.AddHostedService<AndamentoConsumer>();

// ----- API -----
// Enums saem como texto no JSON ("Pendente") em vez de número
builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Libera o front Angular em desenvolvimento (ng serve roda na porta 4200)
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.WithOrigins("http://localhost:4200").AllowAnyHeader().AllowAnyMethod()));

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();

    // Cria a tabela "pedidos" no Supabase se ainda não existir.
    // Mais para frente você pode trocar por migrations (dotnet ef migrations add ...).
    using var escopo = app.Services.CreateScope();
    escopo.ServiceProvider.GetRequiredService<AppDbContext>().Database.Migrate();
}

app.UseCors();

app.MapPedidoEndpoints();

app.Run();