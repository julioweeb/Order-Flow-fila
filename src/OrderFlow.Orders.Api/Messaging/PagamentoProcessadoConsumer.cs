using System.Text.Json;
using OrderFlow.Contracts;
using OrderFlow.Orders.Api.Data;
using OrderFlow.Orders.Api.Domain;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OrderFlow.Orders.Api.Messaging;

// Fecha o ciclo: escuta o resultado do pagamento e atualiza o status do pedido no banco.
public sealed class PagamentoProcessadoConsumer(
    RabbitMqSettings settings,
    IServiceScopeFactory scopeFactory,
    ILogger<PagamentoProcessadoConsumer> logger) : BackgroundService
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

            // O RabbitMQ pode demorar para subir: tenta conectar até conseguir
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

            // Quem consome declara a própria fila e o binding
            await canal.ExchangeDeclareAsync(
                Mensageria.Exchange, ExchangeType.Topic, durable: true, cancellationToken: stoppingToken);
            await canal.QueueDeclareAsync(
                Mensageria.FilaResultadoPagamento, durable: true, exclusive: false, autoDelete: false,
                cancellationToken: stoppingToken);
            await canal.QueueBindAsync(
                Mensageria.FilaResultadoPagamento, Mensageria.Exchange, Mensageria.RkPagamentoQualquer,
                cancellationToken: stoppingToken);

            // Prefetch: no máximo 10 mensagens "em voo" sem ack por vez
            await canal.BasicQosAsync(0, 10, false, stoppingToken);

            var consumidor = new AsyncEventingBasicConsumer(canal);
            consumidor.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    var evento = JsonSerializer.Deserialize<PagamentoProcessado>(ea.Body.Span, Mensageria.Json);
                    if (evento is null)
                    {
                        logger.LogWarning("Mensagem inválida descartada.");
                        await canal.BasicNackAsync(ea.DeliveryTag, false, false, stoppingToken);
                        return;
                    }

                    // BackgroundService é singleton e o DbContext é scoped: cria um escopo por mensagem
                    using var escopo = scopeFactory.CreateScope();
                    var db = escopo.ServiceProvider.GetRequiredService<AppDbContext>();

                    var pedido = await db.Pedidos.FindAsync(new object[] { evento.PedidoId }, stoppingToken);
                    if (pedido is null)
                    {
                        logger.LogWarning("Pedido {PedidoId} não encontrado.", evento.PedidoId);
                    }
                    // Idempotência básica: se o pedido já saiu de "Pendente", não processa de novo
                    else if (pedido.Status == StatusPedido.Pendente)
                    {
                        pedido.Status = evento.Aprovado
                            ? StatusPedido.PagamentoAprovado
                            : StatusPedido.PagamentoRecusado;
                        pedido.MotivoRecusa = evento.Motivo;
                        db.Eventos.Add(PedidoEvento.Novo(pedido.Id, "Orders.Api", $"Resultado recebido da fila. Status atualizado para {pedido.Status} no banco"));
                        await db.SaveChangesAsync(stoppingToken);

                        logger.LogInformation("Pedido {PedidoId} atualizado para {Status}.", pedido.Id, pedido.Status);
                    }

                    // Ack só depois de tudo certo: se der erro antes, a mensagem não se perde
                    await canal.BasicAckAsync(ea.DeliveryTag, false, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Aplicação encerrando: sem ack, o RabbitMQ devolve a mensagem para a fila
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Erro ao processar resultado de pagamento.");
                    // requeue: false => descarta (na fase 4 vai para retry + Dead Letter Queue)
                    await canal.BasicNackAsync(ea.DeliveryTag, false, false, stoppingToken);
                }
            };

            await canal.BasicConsumeAsync(
                Mensageria.FilaResultadoPagamento, autoAck: false, consumer: consumidor,
                cancellationToken: stoppingToken);

            logger.LogInformation("Aguardando resultados de pagamento na fila {Fila}...", Mensageria.FilaResultadoPagamento);
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Encerramento normal da aplicação
        }
    }
}