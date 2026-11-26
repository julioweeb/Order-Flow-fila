using System.Text.Json;

namespace OrderFlow.Contracts;

// Nomes compartilhados entre os serviços: exchange, routing keys e filas.
// Centralizar aqui evita erro de digitação em "strings mágicas".
public static class Mensageria
{
    // Exchange do tipo "topic": roteia as mensagens pelo padrão da routing key
    public const string Exchange = "orderflow.events";

    // Routing keys (o "assunto" de cada evento)
    public const string RkPedidoCriado = "pedido.criado";
    public const string RkPagamentoAprovado = "pagamento.aprovado";
    public const string RkPagamentoRecusado = "pagamento.recusado";

    // O "*" casa com uma palavra: pega tanto aprovado quanto recusado
    public const string RkPagamentoQualquer = "pagamento.*";

    // Cada consumidor tem a sua própria fila
    public const string FilaPagamento = "payment.pedido-criado";
    public const string FilaResultadoPagamento = "orders.pagamento-processado";

    // ----- Retry com backoff e Dead Letter Queue -----

    // Atraso (em segundos) de cada nova tentativa. O tamanho do array = máximo de retries.
    public static readonly int[] AtrasosRetrySegundos = [5, 15, 30];

    // Fila onde ficam as mensagens que esgotaram as tentativas
    public const string FilaPagamentoDlq = "payment.pedido-criado.dlq";

    // Uma fila de espera para cada atraso (ex.: payment.pedido-criado.retry.5s)
    public static string FilaPagamentoRetry(int segundos) => $"{FilaPagamento}.retry.{segundos}s";

    // Cabeçalhos que acompanham a mensagem nas tentativas
    public const string HeaderTentativas = "x-tentativas";
    public const string HeaderErro = "x-erro";

    // Mesmo formato JSON (camelCase) para quem publica e para quem consome
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
}