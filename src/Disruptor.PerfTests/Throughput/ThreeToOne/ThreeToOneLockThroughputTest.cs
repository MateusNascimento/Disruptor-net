using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Disruptor.PerfTests.Support;

namespace Disruptor.PerfTests.Throughput.ThreeToOne;

public class ThreeToOneLockThroughputTest : IThroughputTest, IExternalTest
{
    private const int _publisherCount = 3;
    private const long _iterations = 1000 * 1000 * 20;
    private readonly Barrier _signal = new(_publisherCount + 1);
    private readonly PerfAdditionLockEventProcessor _lockProcessor;
    private readonly PerfLockProducer[] _perfQueueProducers = new PerfLockProducer[_publisherCount];

    public ThreeToOneLockThroughputTest()
    {
        _lockProcessor = new PerfAdditionLockEventProcessor(((_iterations / _publisherCount) * _publisherCount) - 1L);
        for (var i = 0; i < _publisherCount; i++)
        {
            _perfQueueProducers[i] = new PerfLockProducer(_signal, _lockProcessor, _iterations / _publisherCount);
        }
    }

    public int RequiredProcessorCount => 4;

    public long Run(ThroughputSessionContext sessionContext)
    {
        var signal = new ManualResetEvent(false);
        _lockProcessor.Reset(signal);

        var tasks = new Task[_publisherCount];
        for (var i = 0; i < _publisherCount; i++)
        {
            tasks[i] = _perfQueueProducers[i].Start();
        }

        _lockProcessor.Start();

        sessionContext.Start();
        _signal.SignalAndWait();
        Task.WaitAll(tasks);
        signal.WaitOne();
        sessionContext.Stop();
        _lockProcessor.Halt();

        return _iterations;
    }
}
