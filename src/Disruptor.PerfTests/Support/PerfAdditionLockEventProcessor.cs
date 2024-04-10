using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Disruptor.PerfTests.Support;

public class PerfAdditionLockEventProcessor
{
    private volatile bool _running;
    private long _value;
    private long _sequence;
    private ManualResetEvent _signal;

    private object _lock = new();

    private readonly long _count;

    public PerfAdditionLockEventProcessor(long count)
    {
        _count = count;
    }

    public long Value => _value;

    public void Reset(ManualResetEvent signal)
    {
        _value = 0L;
        _sequence = 0L;
        _signal = signal;
    }

    public void Halt() => _running = false;

    public void Process(ref long value)
    {
        if (!_running) { return; }

        lock (_lock)
        {
            _value += value;

            if (_sequence++ == _count)
            {
                _signal.Set();
            }
        }
    }

    public void Start()
    {
        _running = true;
    }
}
