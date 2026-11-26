using System.Text.Json;
using OrderFlow.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OrderFlow.Payment.Worker;

// Consome "pedido.criado", simula a cobrança e publica o resultado.
// Se algo falhar: retry com backoff (5s, 15s, 30s) e, esgotadas as tentativas, Dead Letter Queue.
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

                try
                {
                    var pedido = JsonSerializer.Deserialize<PedidoCriado>(ea.Body.Span, Mensageria.Json)
                                 ?? throw new JsonException("Corpo da mensagem vazio.");

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
                        await TratarFalhaAsync(conexao, canal, ea, tentativa, ex, stoppingToken);
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

        // Fila principal (igual à de antes: sem argumentos extras)
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

        // Filas de espera: ninguém consome. Quando o TTL expira, a mensagem é "dead-lettered"
        // para o exchange padrão ("") com a routing key da fila principal, ou seja, volta para ela.
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
    //   "falha"    -> sempre falha (vai passar por todos os retries e cair na DLQ)
    //   "instavel" -> falha nas 2 primeiras tentativas e depois funciona (retry resolvendo)
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
        int tentativa, Exception ex, CancellationToken ct)
    {
        var atrasos = Mensageria.AtrasosRetrySegundos;

        if (tentativa < atrasos.Length)
        {
            var segundos = atrasos[tentativa];
            logger.LogWarning("Falha na tentativa {Tentativa}: {Erro}. Nova tentativa em {Segundos}s.",
                tentativa + 1, ex.Message, segundos);

            await RepublicarAsync(conexao, Mensageria.FilaPagamentoRetry(segundos), ea, tentativa + 1, ex.Message, ct);
        }
        else
        {
            logger.LogError("Esgotou as tentativas ({Total}): {Erro}. Enviando para a DLQ.",
                tentativa + 1, ex.Message);

            await RepublicarAsync(conexao, Mensageria.FilaPagamentoDlq, ea, tentativa, ex.Message, ct);
        }

        // Só confirma a original DEPOIS de a cópia estar na fila de destino.
        // Se o processo cair no meio, a mensagem pode chegar duplicada, mas nunca se perde
        // (por isso o consumidor precisa ser idempotente).
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