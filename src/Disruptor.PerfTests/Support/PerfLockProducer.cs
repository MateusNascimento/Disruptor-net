using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Disruptor.PerfTests.Support;

public class PerfLockProducer
{
    private readonly Barrier _barrier;
    private readonly PerfAdditionLockEventProcessor _processor;
    private readonly long _iterations;

    public PerfLockProducer(Barrier barrier, PerfAdditionLockEventProcessor processor, long iterations)
    {
        _barrier = barrier;
        _processor = processor;
        _iterations = iterations;
    }

    public void Run()
    {
        _barrier.SignalAndWait();
        for (long i = 0; i < _iterations; i++)
        {
            _processor.Process(ref i);
        }
    }

    public Task Start()
    {
        return PerfTestUtil.StartLongRunning(Run);
    }
}
