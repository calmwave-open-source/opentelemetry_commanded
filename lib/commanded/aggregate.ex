defmodule OpentelemetryCommanded.Aggregate do
  @moduledoc false

  require OpenTelemetry.Tracer

  import OpentelemetryCommanded.Util

  alias OpenTelemetry.Span

  @tracer_id __MODULE__

  def setup() do
    :telemetry.attach(
      {__MODULE__, :command_start},
      [:commanded, :aggregate, :execute, :start],
      &__MODULE__.handle_command_start/4,
      []
    )

    :telemetry.attach(
      {__MODULE__, :command_stop},
      [:commanded, :aggregate, :execute, :stop],
      &__MODULE__.handle_command_stop/4,
      []
    )

    :telemetry.attach(
      {__MODULE__, :command_exception},
      [:commanded, :aggregate, :execute, :exception],
      &__MODULE__.handle_command_exception/4,
      []
    )

    :telemetry.attach(
      {__MODULE__, :populate_start},
      [:commanded, :aggregate, :populate, :start],
      &__MODULE__.handle_populate_start/4,
      []
    )

    :telemetry.attach(
      {__MODULE__, :populate_stop},
      [:commanded, :aggregate, :populate, :stop],
      &__MODULE__.handle_populate_stop/4,
      []
    )

  end

  def handle_command_start(_event, _, meta, _) do
    context = meta.execution_context

    safe_context_propagation(context.metadata["trace_ctx"])

    attributes = [
      "commanded.aggregate_uuid": meta.aggregate_uuid,
      "commanded.aggregate_version": meta.aggregate_version,
      "commanded.application": meta.application,
      "commanded.causation_id": context.causation_id,
      "commanded.command": struct_name(context.command),
      "commanded.correlation_id": context.correlation_id,
      "commanded.function": context.function,
      "messaging.conversation_id": context.correlation_id,
      "messaging.destination": context.handler,
      "messaging.destination_kind": "aggregate",
      "messaging.message_id": context.causation_id,
      "messaging.operation": "receive",
      "messaging.system": "commanded"
    ]

    OpentelemetryTelemetry.start_telemetry_span(
      @tracer_id,
      "commanded.aggregate.execute",
      meta,
      %{
        kind: :consumer,
        attributes: attributes
      }
    )
  end

  def handle_command_stop(_event, _measurements, meta, _) do
    # ensure the correct span is current
    ctx = OpentelemetryTelemetry.set_current_telemetry_span(@tracer_id, meta)

    events = Map.get(meta, :events, [])
    Span.set_attribute(ctx, :"commanded.event_count", Enum.count(events))

    if error = meta[:error] do
      Span.set_status(ctx, OpenTelemetry.status(:error, inspect(error)))
    end

    OpentelemetryTelemetry.end_telemetry_span(@tracer_id, meta)
  end

  def handle_command_exception(
        _event,
        _measurements,
        %{kind: kind, reason: reason, stacktrace: stacktrace} = meta,
        _config
      ) do
    ctx = OpentelemetryTelemetry.set_current_telemetry_span(@tracer_id, meta)

    # try to normalize all errors to Elixir exceptions
    exception = Exception.normalize(kind, reason, stacktrace)

    # record exception and mark the span as errored
    Span.record_exception(ctx, exception, stacktrace)
    Span.set_status(ctx, OpenTelemetry.status(:error, inspect(reason)))

    OpentelemetryTelemetry.end_telemetry_span(@tracer_id, meta)
  end

  def handle_populate_start(_event, _, meta, _) do
    context = meta.execution_context

    safe_context_propagation(context.metadata["trace_ctx"])

    attributes = [
      "commanded.aggregate_uuid": meta.aggregate_uuid,
      "commanded.aggregate_version": meta.aggregate_version,
      "commanded.application": meta.application,
      "commanded.function": context.function
    ]

    OpentelemetryTelemetry.start_telemetry_span(
      @tracer_id,
      "commanded.aggregate.pupulate",
      meta,
      %{
        kind: :consumer,
        attributes: attributes
      }
    )
  end

  def handle_populate_stop(event, measurements, meta, _) do
    IO.inspect(event, label: "event")
    IO.inspect(measurements, label: "measurements")
    IO.inspect(meta, label: "meta")

    OpentelemetryTelemetry.end_telemetry_span(@tracer_id, meta)
  end

end
