using System.Text.Json;
using OrderFlow.Contracts;
using OrderFlow.Orders.Api.Data;
using OrderFlow.Orders.Api.Domain;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OrderFlow.Orders.Api.Messaging;

// Escuta os eventos de acompanhamento (andamento.pedido) e grava na linha do tempo do pedido.
public sealed class AndamentoConsumer(
    RabbitMqSettings settings,
    IServiceScopeFactory scopeFactory,
    ILogger<AndamentoConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var factory = new ConnectionFactory
            {
                HostName = settings.HostName,
                Port = settings.Port,
                UserName = settings.UserName,
                Password = settings.Password
            };

            IConnection conexao;
            while (true)
            {
                try
                {
                    conexao = await factory.CreateConnectionAsync(stoppingToken);
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning("RabbitMQ indisponível ({Mensagem}). Tentando de novo em 3s...", ex.Message);
                    await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
                }
            }

            await using var _ = conexao;
            await using var canal = await conexao.CreateChannelAsync(cancellationToken: stoppingToken);

            await canal.ExchangeDeclareAsync(
                Mensageria.Exchange, ExchangeType.Topic, durable: true, cancellationToken: stoppingToken);
            await canal.QueueDeclareAsync(
                Mensageria.FilaAndamento, durable: true, exclusive: false, autoDelete: false,
                cancellationToken: stoppingToken);
            await canal.QueueBindAsync(
                Mensageria.FilaAndamento, Mensageria.Exchange, Mensageria.RkPedidoAndamento,
                cancellationToken: stoppingToken);

            await canal.BasicQosAsync(0, 10, false, stoppingToken);

            var consumidor = new AsyncEventingBasicConsumer(canal);
            consumidor.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    var andamento = JsonSerializer.Deserialize<PedidoAndamento>(ea.Body.Span, Mensageria.Json);
                    if (andamento is not null)
                    {
                        using var escopo = scopeFactory.CreateScope();
                        var db = escopo.ServiceProvider.GetRequiredService<AppDbContext>();

                        db.Eventos.Add(PedidoEvento.Novo(
                            andamento.PedidoId, andamento.Origem, andamento.Mensagem, andamento.OcorridoEmUtc));
                        await db.SaveChangesAsync(stoppingToken);
                    }

                    await canal.BasicAckAsync(ea.DeliveryTag, false, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Encerrando: sem ack, a mensagem volta para a fila
                }
                catch (Exception ex)
                {
                    // Acompanhamento não é crítico: se falhar, descarta e segue (sem requeue)
                    logger.LogWarning("Não foi possível gravar o andamento: {Erro}", ex.Message);
                    await canal.BasicNackAsync(ea.DeliveryTag, false, false, stoppingToken);
                }
            };

            await canal.BasicConsumeAsync(
                Mensageria.FilaAndamento, autoAck: false, consumer: consumidor,
                cancellationToken: stoppingToken);

            logger.LogInformation("Aguardando eventos de andamento na fila {Fila}...", Mensageria.FilaAndamento);
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Encerramento normal
        }
    }
}