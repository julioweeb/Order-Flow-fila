using Microsoft.EntityFrameworkCore;
using OrderFlow.Orders.Api.Domain;

namespace OrderFlow.Orders.Api.Data;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Pedido> Pedidos => Set<Pedido>(); // Propriedade DbSet para a entidade Pedido
    public DbSet<PedidoEvento> Eventos => Set<PedidoEvento>(); // Propriedade DbSet para a entidade PedidoEvento

    protected override void OnModelCreating(ModelBuilder modelBuilder) // Configurações adicionais do modelo
    {
        modelBuilder.Entity<Pedido>(e =>
        {
            e.ToTable("pedidos");
            e.HasKey(p => p.Id);
            e.Property(p => p.Cliente).IsRequired().HasMaxLength(200);
            e.Property(p => p.Valor).HasPrecision(18, 2);
            e.Property(p => p.Status).HasConversion<string>().HasMaxLength(30);
            e.Property(p => p.MotivoRecusa).HasMaxLength(300);
        });

        modelBuilder.Entity<PedidoEvento>(e =>
        {
            e.ToTable("pedido_eventos");
            e.HasKey(x => x.Id);
            e.Property(x => x.Origem).IsRequired().HasMaxLength(50);
            e.Property(x => x.Mensagem).IsRequired().HasMaxLength(500);
            e.HasIndex(x => x.PedidoId); // a consulta é sempre "eventos deste pedido"
        });
    }
}