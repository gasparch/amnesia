#            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
#                    Version 2, December 2004
#
#            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
#   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION
#
#  0. You just DO WHAT THE FUCK YOU WANT TO.

defmodule Amnesia.Table.StreamWhere do
  defstruct table: nil, spec: nil, lock: :read, dirty: false, cont: nil, pos: 0

  alias __MODULE__, as: SW
  alias Amnesia.Selection, as: S

  def new(name, spec, options) do
    lock    = Keyword.get(options, :lock,    :read)
    dirty   = Keyword.get(options, :dirty,   false)

    stream = %SW{table: name, spec: spec, lock: lock, dirty: dirty}

    cont = first(stream)

    if cont do
      %{stream | cont: cont}
    else
      []
    end
  end

  defp first(%SW{table: table, spec: spec, dirty: false}) do
    table.select(1, spec)
  end

#  defp first(%SW{table: table, dirty: true, reverse: false}) do
#    table.first!(true)
#  end
#
#  defp first(%SW{table: table, dirty: true, reverse: true}) do
#    table.last!(true)
#  end

  defp next(cont) do
    S.next(cont)
  end

  defp read(cont) do
    hd S.values cont
  end

  @doc false
  def reduce(stream, acc, fun) do
    reduce(stream, stream.cont, acc, fun)
  end

  defp reduce(_stream, _val, { :halt, acc }, _fun) do
    { :halted, acc }
  end

  defp reduce(stream, cont, { :suspend, acc }, fun) do
    { :suspended, acc, &reduce(stream, cont, &1, fun) }
  end

  defp reduce(_stream, nil, { :cont, acc }, _fun) do
    { :done, acc }
  end

  defp reduce(stream, cont, { :cont, acc }, fun) do
    reduce(stream, next(cont), fun.(read(cont), acc), fun)
  end

  defimpl Enumerable do
    def reduce(stream, acc, fun) do
      Amnesia.Table.StreamWhere.reduce(stream, acc, fun)
    end

    def count(_) do
      { :error, __MODULE__ }
    end

    def member?(_, _) do
      { :error, __MODULE__ }
    end
  end
end
