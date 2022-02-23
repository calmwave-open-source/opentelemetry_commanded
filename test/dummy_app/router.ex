defmodule OpentelemetryCommanded.DummyApp.Router do
  @moduledoc false

  use Commanded.Commands.Router

  alias OpentelemetryCommanded.DummyApp.Aggregate
  alias OpentelemetryCommanded.DummyApp.Commands, as: C
  alias OpentelemetryCommanded.DummyApp.Handler
  alias Commanded.Helpers.CommandAuditMiddleware

  middleware(CommandAuditMiddleware)
  middleware(OpentelemetryCommanded.Middleware)
  middleware(OpentelemetryCommanded.DummyApp.CommandValidatorMiddleware)

  identify(Aggregate, by: :id)

  dispatch([C.Ok, C.Error, C.RaiseException, C.DoEvent, C.DispatchError],
    to: Handler,
    aggregate: Aggregate
  )
end
