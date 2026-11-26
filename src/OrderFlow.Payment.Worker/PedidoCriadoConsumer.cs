using System.Text.Json;
using OrderFlow.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OrderFlow.Payment.Worker;

// Consome "pedido.criado", simula a cobrança e publica o resultado.
// Se algo falhar: retry com backoff (5s, 15s, 30s) e, esgotadas as tentativas, Dead Letter Queue.
// Cada etapa também é publicada como "andamento.pedido" para aparecer na linha do tempo do front.
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

            await DeclararTopologiaAsync(canal, stoppingToken);

            // Prefetch: até 5 pedidos ao mesmo tempo sem ack
            await canal.BasicQosAsync(0, 5, false, stoppingToken);

            var consumidor = new AsyncEventingBasicConsumer(canal);
            consumidor.ReceivedAsync += async (_, ea) =>
            {
                // Quantas vezes esta mensagem já falhou (0 na primeira entrega)
                var tentativa = LerTentativas(ea);
                PedidoCriado? pedido = null;

                try
                {
                    pedido = JsonSerializer.Deserialize<PedidoCriado>(ea.Body.Span, Mensageria.Json)
                             ?? throw new JsonException("Corpo da mensagem vazio.");

                    await RegistrarAndamentoAsync(conexao, pedido.PedidoId,
                        $"Pagamento iniciado (tentativa {tentativa + 1})", stoppingToken);

                    logger.LogInformation("Processando pagamento do pedido {PedidoId} ({Valor:C}) - tentativa {Tentativa}...",
                        pedido.PedidoId, pedido.Valor, tentativa + 1);

                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken); // simula o gateway de pagamento

                    SimularFalhaDoGateway(pedido, tentativa);

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

                    await RegistrarAndamentoAsync(conexao, pedido.PedidoId,
                        aprovado
                            ? "Pagamento aprovado. Resultado publicado (pagamento.aprovado)"
                            : "Pagamento recusado: valor acima do limite. Resultado publicado (pagamento.recusado)",
                        stoppingToken);

                    logger.LogInformation("Pedido {PedidoId}: pagamento {Resultado}.",
                        pedido.PedidoId, aprovado ? "APROVADO" : "RECUSADO");

                    // Ack só depois de tudo certo
                    await canal.BasicAckAsync(ea.DeliveryTag, false, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Aplicação encerrando: sem ack, o RabbitMQ devolve a mensagem para a fila
                }
                catch (JsonException ex)
                {
                    // Mensagem malformada: tentar de novo não adianta, vai direto para a DLQ
                    logger.LogError("Mensagem inválida enviada para a DLQ: {Erro}", ex.Message);
                    await RepublicarAsync(conexao, Mensageria.FilaPagamentoDlq, ea, tentativa, ex.Message, stoppingToken);
                    await canal.BasicAckAsync(ea.DeliveryTag, false, stoppingToken);
                }
                catch (Exception ex)
                {
                    try
                    {
                        await TratarFalhaAsync(conexao, canal, ea, tentativa, pedido?.PedidoId, ex, stoppingToken);
                    }
                    catch (Exception erroAoReenfileirar)
                    {
                        // Nem o reenfileiramento funcionou: devolve a original para a fila e não perde a mensagem
                        logger.LogError(erroAoReenfileirar, "Não foi possível reenfileirar a mensagem.");
                        await canal.BasicNackAsync(ea.DeliveryTag, false, true, stoppingToken);
                    }
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

    // Cria exchange, fila principal, DLQ e as filas de espera do retry
    private static async Task DeclararTopologiaAsync(IChannel canal, CancellationToken ct)
    {
        await canal.ExchangeDeclareAsync(
            Mensageria.Exchange, ExchangeType.Topic, durable: true, cancellationToken: ct);

        // Fila principal
        await canal.QueueDeclareAsync(
            Mensageria.FilaPagamento, durable: true, exclusive: false, autoDelete: false,
            cancellationToken: ct);
        await canal.QueueBindAsync(
            Mensageria.FilaPagamento, Mensageria.Exchange, Mensageria.RkPedidoCriado,
            cancellationToken: ct);

        // Dead Letter Queue: guarda o que esgotou as tentativas
        await canal.QueueDeclareAsync(
            Mensageria.FilaPagamentoDlq, durable: true, exclusive: false, autoDelete: false,
            cancellationToken: ct);

        // Filas de espera: ninguém consome. Quando o TTL expira, a mensagem volta para a fila principal.
        foreach (var segundos in Mensageria.AtrasosRetrySegundos)
        {
            var argumentos = new Dictionary<string, object?>
            {
                ["x-message-ttl"] = segundos * 1000,
                ["x-dead-letter-exchange"] = "",
                ["x-dead-letter-routing-key"] = Mensageria.FilaPagamento
            };

            await canal.QueueDeclareAsync(
                Mensageria.FilaPagamentoRetry(segundos), durable: true, exclusive: false, autoDelete: false,
                arguments: argumentos, cancellationToken: ct);
        }
    }

    // Só para o demo: o nome do cliente define o comportamento do "gateway"
    //   "falha"    -> sempre falha (passa por todos os retries e cai na DLQ)
    //   "instavel" -> falha nas 2 primeiras tentativas e depois funciona
    private static void SimularFalhaDoGateway(PedidoCriado pedido, int tentativa)
    {
        var nome = pedido.Cliente.ToLowerInvariant();

        if (nome.Contains("falha"))
            throw new InvalidOperationException("Gateway de pagamento indisponível (simulado).");

        if (nome.Contains("instavel") && tentativa < 2)
            throw new InvalidOperationException("Gateway instável (simulado).");
    }

    // Decide: tenta de novo (fila de espera) ou desiste (DLQ)
    private async Task TratarFalhaAsync(
        IConnection conexao, IChannel canal, BasicDeliverEventArgs ea,
        int tentativa, Guid? pedidoId, Exception ex, CancellationToken ct)
    {
        var atrasos = Mensageria.AtrasosRetrySegundos;

        if (tentativa < atrasos.Length)
        {
            var segundos = atrasos[tentativa];
            logger.LogWarning("Falha na tentativa {Tentativa}: {Erro}. Nova tentativa em {Segundos}s.",
                tentativa + 1, ex.Message, segundos);

            if (pedidoId is { } id)
                await RegistrarAndamentoAsync(conexao, id,
                    $"Falha na tentativa {tentativa + 1}: {ex.Message} Nova tentativa em {segundos}s (fila de espera)", ct);

            await RepublicarAsync(conexao, Mensageria.FilaPagamentoRetry(segundos), ea, tentativa + 1, ex.Message, ct);
        }
        else
        {
            logger.LogError("Esgotou as tentativas ({Total}): {Erro}. Enviando para a DLQ.",
                tentativa + 1, ex.Message);

            if (pedidoId is { } id)
                await RegistrarAndamentoAsync(conexao, id,
                    $"Falha na tentativa {tentativa + 1}: {ex.Message} Tentativas esgotadas, mensagem enviada para a DLQ", ct);

            await RepublicarAsync(conexao, Mensageria.FilaPagamentoDlq, ea, tentativa, ex.Message, ct);
        }

        // Só confirma a original DEPOIS de a cópia estar na fila de destino
        await canal.BasicAckAsync(ea.DeliveryTag, false, ct);
    }

    private static int LerTentativas(BasicDeliverEventArgs ea)
    {
        if (ea.BasicProperties.Headers is { } headers
            && headers.TryGetValue(Mensageria.HeaderTentativas, out var valor)
            && valor is not null)
        {
            return Convert.ToInt32(valor);
        }

        return 0;
    }

    // Publica uma etapa para a linha do tempo. É "melhor esforço": nunca pode derrubar o pagamento.
    private async Task RegistrarAndamentoAsync(
        IConnection conexao, Guid pedidoId, string mensagem, CancellationToken ct)
    {
        try
        {
            var andamento = new PedidoAndamento(pedidoId, "Payment.Worker", mensagem, DateTime.UtcNow);
            await PublicarAsync(conexao, Mensageria.RkPedidoAndamento, andamento, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Não foi possível registrar o andamento: {Erro}", ex.Message);
        }
    }

    // Copia a mensagem (mesmo corpo e MessageId) para outra fila, registrando tentativas e erro
    private static async Task RepublicarAsync(
        IConnection conexao, string fila, BasicDeliverEventArgs ea,
        int tentativas, string erro, CancellationToken ct)
    {
        await using var canal = await conexao.CreateChannelAsync(cancellationToken: ct);

        var propriedades = new BasicProperties
        {
            Persistent = true,
            ContentType = ea.BasicProperties.ContentType,
            MessageId = ea.BasicProperties.MessageId,
            Headers = new Dictionary<string, object?>
            {
                [Mensageria.HeaderTentativas] = tentativas,
                [Mensageria.HeaderErro] = erro
            }
        };

        // Exchange "" = padrão do RabbitMQ: entrega direto na fila cujo nome é a routing key
        await canal.BasicPublishAsync(
            exchange: "",
            routingKey: fila,
            mandatory: false,
            basicProperties: propriedades,
            body: ea.Body,
            cancellationToken: ct);
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
            Persistent = true,
            ContentType = "application/json",
            MessageId = Guid.NewGuid().ToString()
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