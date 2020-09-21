using System;
using System.Threading.Tasks;
using MassTransit;
using MassTransit.Context;

namespace MassTransitTest.Misc
{
    public static class ConsumeContextExtensions
    {
        public static void AddExplicitOutboxAction(this ConsumeContext context, Func<Task> action)
        {
            if (!context.TryGetPayload<InMemoryOutboxConsumeContext>(out var outboxContext))
                throw new InvalidOperationException("Outbox context is not available at this point");

            outboxContext.Add(action);
        }
    }
}