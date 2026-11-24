using System.Reflection.Metadata.Ecma335;

namespace OrderFlow.Orders.Api.Domain
{
    public enum StatusPedido
    {
        Pendente,
        PagamentoAprovado,
        PagamentoRecusado
    }

    public class Pedido
    {
        public Guid Id { get; set; }
        public string Cliente { get; set; } = string.Empty;
        public decimal Valor { get; set; }
        public StatusPedido Status { get; set; } = StatusPedido.Pendente;
        public string? MotivoRecusa { get; set; }
        public DateTime CriadoEmUtc { get; set; }
    }
}