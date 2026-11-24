using OrderFlow.Contracts;
using OrderFlow.Payment.Worker;

var builder = Host.CreateApplicationBuilder(args);

// Lê a seção "RabbitMq" do appsettings.json
var rabbit = builder.Configuration.GetSection("RabbitMq").Get<RabbitMqSettings>() ?? new RabbitMqSettings();
builder.Services.AddSingleton(rabbit);
builder.Services.AddHostedService<PedidoCriadoConsumer>();

await builder.Build().RunAsync();