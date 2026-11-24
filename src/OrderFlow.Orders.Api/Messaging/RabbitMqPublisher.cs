using System.Text.Json;
using OrderFlow.Contracts;
using RabbitMQ.Client;

namespace OrderFlow.Orders.Api.Messaging;

// Publica eventos no RabbitMQ. A conexão é única (singleton) e reaproveitada;
// o canal é criado a cada publicação, o que é simples e seguro para threads.
public sealed class RabbitMqPublisher(RabbitMqSettings settings) : IAsyncDisposable
{
    private readonly SemaphoreSlim _trava = new(1, 1); // garante que apenas uma thread cria a conexão de cada vez
    private IConnection? _conexao; 

    private async Task<IConnection> ObterConexaoAsync(CancellationToken ct) // cria a conexão se ainda não existir ou estiver fechada
    {
        if (_conexao is { IsOpen: true }) return _conexao;

        await _trava.WaitAsync(ct);
        try
        {
            if (_conexao is { IsOpen: true }) return _conexao;

            var factory = new ConnectionFactory
            {
                HostName = settings.HostName,
                Port = settings.Port,
                UserName = settings.UserName,
                Password = settings.Password
            };
            _conexao = await factory.CreateConnectionAsync(ct);
            return _conexao;
        }
        finally
        {
            _trava.Release();
        }
    }

    public async Task PublicarAsync<T>(string routingKey, T evento, CancellationToken ct = default)
    {
        var conexao = await ObterConexaoAsync(ct);
        await using var canal = await conexao.CreateChannelAsync(cancellationToken: ct);

        // Declarar é idempotente: se já existir igual, não faz nada
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

    public async ValueTask DisposeAsync()
    {
        if (_conexao is not null) await _conexao.DisposeAsync();
        _trava.Dispose();
    }
}