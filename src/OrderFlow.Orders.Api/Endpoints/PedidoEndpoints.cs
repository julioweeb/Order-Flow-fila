using Microsoft.EntityFrameworkCore;
using OrderFlow.Contracts;
using OrderFlow.Orders.Api.Data;
using OrderFlow.Orders.Api.Domain;
using OrderFlow.Orders.Api.Messaging;

namespace OrderFlow.Orders.Api.Endpoints;

public sealed record CriarPedidoRequest(string Cliente, decimal Valor);

public static class PedidoEndpoints
{
    // Registra todas as rotas de /pedidos de uma vez
    public static IEndpointRouteBuilder MapPedidoEndpoints(this IEndpointRouteBuilder app)
    {
        var grupo = app.MapGroup("/pedidos").WithTags("Pedidos");

        grupo.MapPost("/", CriarAsync);
        grupo.MapGet("/{id:guid}", ObterPorIdAsync);
        grupo.MapGet("/", ListarAsync);
        grupo.MapGet("/{id:guid}/eventos", ListarEventosAsync);

        return app;
    }

    // Cria o pedido e publica o evento. Responde 202: o pagamento ainda vai ser processado.
    private static async Task<IResult> CriarAsync(
        CriarPedidoRequest req,
        AppDbContext db,
        RabbitMqPublisher publisher,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(req.Cliente) || req.Valor <= 0)
            return Results.BadRequest(new { erro = "Informe o cliente e um valor maior que zero." });

        var pedido = new Pedido
        {
            Id = Guid.NewGuid(),
            Cliente = req.Cliente.Trim(),
            Valor = req.Valor,
            Status = StatusPedido.Pendente,
            CriadoEmUtc = DateTime.UtcNow
        };

        db.Pedidos.Add(pedido);
        db.Eventos.Add(PedidoEvento.Novo(pedido.Id, "Orders.Api",
            "Pedido recebido e salvo no banco (Supabase)", pedido.CriadoEmUtc));
        await db.SaveChangesAsync(ct);

        // Marca a hora ANTES de publicar: assim a ordem na linha do tempo não depende da corrida com o worker
        var publicadoEm = DateTime.UtcNow;

        // ATENÇÃO (problema clássico do "dual write"): se a publicação falhar aqui, o pedido
        // fica salvo mas o evento nunca sai. A solução é o Outbox Pattern.
        await publisher.PublicarAsync(
            Mensageria.RkPedidoCriado,
            new PedidoCriado(pedido.Id, pedido.Cliente, pedido.Valor, pedido.CriadoEmUtc),
            ct);

        db.Eventos.Add(PedidoEvento.Novo(pedido.Id, "Orders.Api",
            "Evento pedido.criado publicado no RabbitMQ", publicadoEm));
        await db.SaveChangesAsync(ct);

        return Results.Accepted($"/pedidos/{pedido.Id}", pedido);
    }

    private static async Task<IResult> ListarEventosAsync(Guid id, AppDbContext db, CancellationToken ct)
    {
        var eventos = await db.Eventos.AsNoTracking()
            .Where(e => e.PedidoId == id)
            .OrderBy(e => e.OcorridoEmUtc)
            .ToListAsync(ct);

        return Results.Ok(eventos);
    }

    private static async Task<IResult> ObterPorIdAsync(Guid id, AppDbContext db, CancellationToken ct)
    {
        var pedido = await db.Pedidos.AsNoTracking().FirstOrDefaultAsync(p => p.Id == id, ct);
        return pedido is null ? Results.NotFound() : Results.Ok(pedido);
    }

    private static async Task<IResult> ListarAsync(AppDbContext db, CancellationToken ct)
    {
        var pedidos = await db.Pedidos.AsNoTracking()
            .OrderByDescending(p => p.CriadoEmUtc)
            .Take(50)
            .ToListAsync(ct);

        return Results.Ok(pedidos);
    }
}