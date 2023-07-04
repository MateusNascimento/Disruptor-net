﻿using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Disruptor.Dsl;
using Disruptor.Util;

namespace Disruptor.Processing;

/// <summary>
/// Convenience class for handling the batching semantics of consuming events from a <see cref="RingBuffer{T}"/>
/// and delegating the available events to an <see cref="IBatchEventHandler{T}"/>.
/// </summary>
/// <remarks>
/// You should probably not use this type directly but instead implement <see cref="IBatchEventHandler{T}"/> and register your handler
/// using <see cref="Disruptor{T}.HandleEventsWith(IBatchEventHandler{T}[])"/>.
/// </remarks>
/// <typeparam name="T">the type of event used.</typeparam>
/// <typeparam name="TDataProvider">the type of the <see cref="IDataProvider{T}"/> used.</typeparam>
/// <typeparam name="TSequenceBarrierOptions">the type of the <see cref="ISequenceBarrierOptions"/> used.</typeparam>
/// <typeparam name="TEventHandler">the type of the <see cref="IBatchEventHandler{T}"/> used.</typeparam>
/// <typeparam name="TBatchSizeLimiter">the type of the <see cref="IBatchSizeLimiter"/> used.</typeparam>
public class AsyncBatchEventProcessor<T, TDataProvider, TSequenceBarrierOptions, TEventHandler, TBatchSizeLimiter> : IAsyncEventProcessor<T>
    where T : class
    where TDataProvider : IDataProvider<T>
    where TSequenceBarrierOptions : ISequenceBarrierOptions
    where TEventHandler : IAsyncBatchEventHandler<T>
    where TBatchSizeLimiter : IBatchSizeLimiter
{
    // ReSharper disable FieldCanBeMadeReadOnly.Local (performance: the runtime type will be a struct)
    private TDataProvider _dataProvider;
    private AsyncSequenceBarrier _sequenceBarrier;
    private TEventHandler _eventHandler;
    private TBatchSizeLimiter _batchSizeLimiter;
    // ReSharper restore FieldCanBeMadeReadOnly.Local

    private readonly Sequence _sequence = new();
    private readonly ManualResetEventSlim _started = new();
    private IExceptionHandler<T> _exceptionHandler = new FatalExceptionHandler<T>();
    private volatile int _runState = ProcessorRunStates.Idle;

    public AsyncBatchEventProcessor(TDataProvider dataProvider, AsyncSequenceBarrier sequenceBarrier, TEventHandler eventHandler, TBatchSizeLimiter batchSizeLimiter)
    {
        _dataProvider = dataProvider;
        _sequenceBarrier = sequenceBarrier;
        _eventHandler = eventHandler;
        _batchSizeLimiter = batchSizeLimiter;

        if (eventHandler is IEventProcessorSequenceAware sequenceAware)
            sequenceAware.SetSequenceCallback(_sequence);
    }

    /// <inheritdoc/>
    public Sequence Sequence => _sequence;

    /// <inheritdoc/>
    public void Halt()
    {
        _runState = ProcessorRunStates.Halted;
        _sequenceBarrier.CancelProcessing();
    }

    /// <inheritdoc/>
    public bool IsRunning => _runState != ProcessorRunStates.Idle;

    /// <inheritdoc/>
    public void SetExceptionHandler(IExceptionHandler<T> exceptionHandler)
    {
        _exceptionHandler = exceptionHandler ?? throw new ArgumentNullException(nameof(exceptionHandler));
    }

    /// <inheritdoc/>
    public void WaitUntilStarted(TimeSpan timeout)
    {
        _started.Wait(timeout);
    }

    /// <inheritdoc/>
    public Task Start(TaskScheduler taskScheduler, TaskCreationOptions taskCreationOptions)
    {
        return Task.Factory.StartNew(async () => await RunAsync(), CancellationToken.None, TaskCreationOptions.None, taskScheduler).Unwrap();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// It is ok to have another thread rerun this method after a halt().
    /// </remarks>
    /// <exception cref="InvalidOperationException">if this object instance is already running in a thread</exception>
    public async Task RunAsync()
    {
#pragma warning disable 420
        var previousRunning = Interlocked.CompareExchange(ref _runState, ProcessorRunStates.Running, ProcessorRunStates.Idle);
#pragma warning restore 420

        if (previousRunning == ProcessorRunStates.Running)
        {
            throw new InvalidOperationException("Thread is already running");
        }

        if (previousRunning == ProcessorRunStates.Idle)
        {
            _sequenceBarrier.ResetProcessing();

            NotifyStart();
            try
            {
                if (_runState == ProcessorRunStates.Running)
                {
                    await ProcessEvents().ConfigureAwait(false);
                }
            }
            finally
            {
                NotifyShutdown();
                _runState = ProcessorRunStates.Idle;
            }
        }
        else
        {
            EarlyExit();
        }
    }

    [MethodImpl(Constants.AggressiveOptimization)]
    private async Task ProcessEvents()
    {
        var nextSequence = _sequence.Value + 1L;
        var availableSequence = _sequence.Value;

        while (true)
        {
            try
            {
                var waitResult = await _sequenceBarrier.WaitForAsync<TSequenceBarrierOptions>(nextSequence).ConfigureAwait(false);
                if (waitResult.IsTimeout)
                {
                    NotifyTimeout();
                    continue;
                }

                availableSequence = _batchSizeLimiter.ApplyMaxBatchSize(waitResult.UnsafeAvailableSequence, nextSequence);
                if (availableSequence >= nextSequence)
                {
                    var batch = _dataProvider.GetBatch(nextSequence, availableSequence);
                    await _eventHandler.OnBatch(batch, nextSequence).ConfigureAwait(false);
                    nextSequence += batch.Length;
                }

                _sequence.SetValue(nextSequence - 1);
            }
            catch (OperationCanceledException) when (_sequenceBarrier.IsCancellationRequested)
            {
                if (_runState != ProcessorRunStates.Running)
                {
                    break;
                }
            }
            catch (Exception ex)
            {
                if (availableSequence >= nextSequence)
                {
                    var batch = _dataProvider.GetBatch(nextSequence, availableSequence);
                    _exceptionHandler.HandleEventException(ex, nextSequence, batch);
                    nextSequence += batch.Length;
                    _sequence.SetValue(nextSequence - 1);
                }
            }
        }
    }

    private void EarlyExit()
    {
        NotifyStart();
        NotifyShutdown();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void NotifyTimeout()
    {
        try
        {
            _eventHandler.OnTimeout(_sequence.Value);
        }
        catch (Exception ex)
        {
            _exceptionHandler.HandleOnTimeoutException(ex, _sequence.Value);
        }
    }

    /// <summary>
    /// Notifies the EventHandler when this processor is starting up
    /// </summary>
    private void NotifyStart()
    {
        try
        {
            _eventHandler.OnStart();
        }
        catch (Exception e)
        {
            _exceptionHandler.HandleOnStartException(e);
        }

        _started.Set();
    }

    /// <summary>
    /// Notifies the EventHandler immediately prior to this processor shutting down
    /// </summary>
    private void NotifyShutdown()
    {
        try
        {
            _eventHandler.OnShutdown();
        }
        catch (Exception e)
        {
            _exceptionHandler.HandleOnShutdownException(e);
        }

        _started.Reset();
    }
}
