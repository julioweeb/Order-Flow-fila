using System.Text.Json;
using OrderFlow.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OrderFlow.Payment.Worker;

// Consome "pedido.criado", simula a cobrança e publica
// "pagamento.aprovado" ou "pagamento.recusado".
public sealed class PedidoCriadoConsumer(
    RabbitMqSettings settings,
    ILogger<PedidoCriadoConsumer> logger) : BackgroundService
{
    // Regra fictícia só para o demo: pedidos acima disso são recusados
    private const decimal LimiteSimulado = 5000m;

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

            // Quem consome declara a própria fila e faz o binding com a exchange
            await canal.ExchangeDeclareAsync(
                Mensageria.Exchange, ExchangeType.Topic, durable: true, cancellationToken: stoppingToken);
            await canal.QueueDeclareAsync(
                Mensageria.FilaPagamento, durable: true, exclusive: false, autoDelete: false,
                cancellationToken: stoppingToken);
            await canal.QueueBindAsync(
                Mensageria.FilaPagamento, Mensageria.Exchange, Mensageria.RkPedidoCriado,
                cancellationToken: stoppingToken);

            // Prefetch: até 5 pedidos ao mesmo tempo sem ack
            await canal.BasicQosAsync(0, 5, false, stoppingToken);

            var consumidor = new AsyncEventingBasicConsumer(canal);
            consumidor.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    var pedido = JsonSerializer.Deserialize<PedidoCriado>(ea.Body.Span, Mensageria.Json);
                    if (pedido is null)
                    {
                        logger.LogWarning("Mensagem inválida descartada.");
                        await canal.BasicNackAsync(ea.DeliveryTag, false, false, stoppingToken);
                        return;
                    }

                    logger.LogInformation("Processando pagamento do pedido {PedidoId} ({Valor:C})...",
                        pedido.PedidoId, pedido.Valor);

                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken); // simula o gateway de pagamento

                    var aprovado = pedido.Valor <= LimiteSimulado;
                    var resultado = new PagamentoProcessado(
                        pedido.PedidoId,
                        aprovado,
                        aprovado ? null : "Valor acima do limite (simulado).",
                        DateTime.UtcNow);

                    await PublicarAsync(
                        conexao,
                        aprovado ? Mensageria.RkPagamentoAprovado : Mensageria.RkPagamentoRecusado,
                        resultado,
                        stoppingToken);

                    logger.LogInformation("Pedido {PedidoId}: pagamento {Resultado}.",
                        pedido.PedidoId, aprovado ? "APROVADO" : "RECUSADO");

                    // Ack só depois de tudo certo: se der erro antes, a mensagem não se perde
                    await canal.BasicAckAsync(ea.DeliveryTag, false, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Aplicação encerrando: sem ack, o RabbitMQ devolve a mensagem para a fila
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Erro ao processar pagamento.");
                    // requeue: false => descarta (na fase 4 vai para retry + Dead Letter Queue)
                    await canal.BasicNackAsync(ea.DeliveryTag, false, false, stoppingToken);
                }
            };

            await canal.BasicConsumeAsync(
                Mensageria.FilaPagamento, autoAck: false, consumer: consumidor,
                cancellationToken: stoppingToken);

            logger.LogInformation("Payment.Worker aguardando pedidos na fila {Fila}...", Mensageria.FilaPagamento);
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Encerramento normal
        }
    }

    // Um canal novo por publicação: a conexão é thread-safe, o canal não é
    private static async Task PublicarAsync<T>(
        IConnection conexao, string routingKey, T evento, CancellationToken ct)
    {
        await using var canal = await conexao.CreateChannelAsync(cancellationToken: ct);
        await canal.ExchangeDeclareAsync(
            Mensageria.Exchange, ExchangeType.Topic, durable: true, cancellationToken: ct);

        var propriedades = new BasicProperties
        {
            Persistent = true,                      // mensagem gravada em disco pelo broker
            ContentType = "application/json",
            MessageId = Guid.NewGuid().ToString()   // útil para idempotência e rastreio
        };

        await canal.BasicPublishAsync(
            exchange: Mensageria.Exchange,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: propriedades,
            body: JsonSerializer.SerializeToUtf8Bytes(evento, Mensageria.Json),
            cancellationToken: ct);
    }
}