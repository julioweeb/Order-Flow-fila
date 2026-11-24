using Microsoft.EntityFrameworkCore;
using OrderFlow.Orders.Api.Domain;

namespace OrderFlow.Orders.Api.Data;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Pedido> Pedidos => Set<Pedido>(); // Propriedade DbSet para a entidade Pedido

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
    }
}