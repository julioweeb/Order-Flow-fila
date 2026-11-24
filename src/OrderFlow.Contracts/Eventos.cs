
namespace OrderFlow.Contracts;

// Publicado pela Orders.Api quando um pedido é criado
public sealed record PedidoCriado(
    Guid PedidoId,
    string Cliente,
    decimal Valor,
    DateTime CriadoEmUtc);

// Publicado pelo Payment.Worker depois de tentar cobrar o pedido
public sealed record PagamentoProcessado(
    Guid PedidoId,
    bool Aprovado,
    string? Motivo,
    DateTime ProcessadoEmUtc);