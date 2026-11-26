namespace OrderFlow.Orders.Api.Domain;

// Uma linha da "linha do tempo" do pedido
public class PedidoEvento
{
    public Guid Id { get; set; }
    public Guid PedidoId { get; set; }
    public string Origem { get; set; } = string.Empty;
    public string Mensagem { get; set; } = string.Empty;
    public DateTime OcorridoEmUtc { get; set; }

    public static PedidoEvento Novo(Guid pedidoId, string origem, string mensagem, DateTime? quando = null) => new()
    {
        Id = Guid.NewGuid(),
        PedidoId = pedidoId,
        Origem = origem,
        // Respeita o limite da coluna (mensagens de erro podem ser longas)
        Mensagem = mensagem.Length > 500 ? mensagem[..500] : mensagem,
        OcorridoEmUtc = quando ?? DateTime.UtcNow
    };
}