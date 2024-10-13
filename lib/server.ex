defmodule Server do
  @moduledoc """
  Implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    args = System.argv()
    Supervisor.start_link([{Task, fn -> Server.listen(args) end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen(args) do
    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    initial_config = setup_config(args)
    loop_acceptor(socket, %{}, initial_config)
  end

  defp loop_acceptor(socket, state, config) do
    {:ok, client} = :gen_tcp.accept(socket)
    serve(%{client: client, state: %{}, config: config})
    loop_acceptor(socket, state, config)
  end

  defp serve(%{client: client, state: state, config: config}) do
    Task.async(fn ->
      {response, {result, new_config}} = read_line(client, state, config)
      write_line(response, client)
      {result, new_config}
      serve(%{client: client, state: result, config: new_config})
    end)
  end

  defp read_line(socket, state, config) do
    {:ok, raw_data} = :gen_tcp.recv(socket, 0)

    {command_data, splitted_data} =
      String.split(raw_data, "\r\n", trim: true) |> Enum.split(3)

    command = command_data |> Enum.join() |> String.upcase()

    execute_command(command, splitted_data, state, config)
  end

  defp write_line(line, socket) do
    :gen_tcp.send(socket, line)
  end

  defp execute_command("*1$4PING", _, state, config) do
    {"+PONG\r\n", {state, config}}
  end

  defp execute_command("*2$4ECHO", [_ | arg], state, config) do
    {"+#{arg}\r\n", {state, config}}
  end

  defp execute_command("*3$3SET", args, state, config) do
    [_, name, _, value] = args

    new_state = Map.put(state, name, %{value: value, expire_time: nil})
    {"+OK\r\n", {new_state, config}}
  end

  defp execute_command("*5$3SET", args, state, config) do
    [_, name, _, value, _, _, _, expiry_milliseconds] = args

    expire_time =
      DateTime.utc_now() |> DateTime.add(String.to_integer(expiry_milliseconds), :millisecond)

    new_state = Map.put(state, name, %{value: value, expire_time: expire_time})

    {"+OK\r\n", {new_state, config}}
  end

  defp execute_command("*2$3GET", [_, arg], state, config) do
    %{value: value, expire_time: expire_time} = Map.get(state, arg)

    if is_nil(expire_time) || expire_time >= DateTime.utc_now() do
      {format_element(value), {state, config}}
    else
      {"$-1\r\n", {state, config}}
    end
  end

  defp execute_command("*3$6CONFIG", [_, "GET" | args], state, config) do
    result =
      args
      |> extract_config_args()
      |> Enum.map(fn field -> fetch_config_field(config, field) end)
      |> Enum.map(fn [k, v] ->
        [format_element(k), format_element(v)]
      end)
      |> List.flatten()

    {"*#{Enum.count(result)}\r\n#{Enum.join(result)}", {state, config}}
  end

  defp fetch_config_field(config, field) do
    [field, Map.fetch!(config, field)]
  end

  defp extract_config_args(args) do
    args
    |> Enum.filter(fn arg -> !String.starts_with?(arg, "$") end)
    |> Enum.map(&String.downcase/1)
  end

  defp setup_config(args) do
    args
    |> Enum.chunk_every(2)
    |> Enum.reduce(%{}, fn [key, value], acc ->
      actual_key = String.replace(key, "--", "")
      Map.put(acc, actual_key, value)
    end)
  end

  defp format_element(element) when is_bitstring(element) do
    "$#{String.length(element)}\r\n#{element}\r\n"
  end
end
