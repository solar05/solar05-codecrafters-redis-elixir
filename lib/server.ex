defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # IO.puts("Logs from your program will appear here!")

    # Uncomment this block to pass the first stage
    #
    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    serve(%{client: client, state: %{}})
    loop_acceptor(socket)
  end

  defp serve(%{client: client, state: state}) do
    Task.async(fn ->
      {response, new_state} = read_line(client, state)
      write_line(response, client)

      serve(%{client: client, state: new_state})
    end)
  end

  defp read_line(socket, state) do
    {:ok, raw_data} = :gen_tcp.recv(socket, 0)

    splitted_data = String.split(raw_data, "\r\n", trim: true)
    command = Enum.take(splitted_data, 3) |> Enum.join() |> String.upcase()

    execute_command(command, splitted_data, state)
  end

  defp write_line(line, socket) do
    :gen_tcp.send(socket, line)
  end

  defp execute_command("*1$4PING", _, state) do
    {"+PONG\r\n", state}
  end

  defp execute_command("*2$4ECHO", [_, _, _, _ | arg], state) do
    {"+#{arg}\r\n", state}
  end

  defp execute_command("*3$3SET", [_, _, _ | args], state) do
    [_, name, _, value] = args

    new_state = Map.put(state, name, %{value: value, expire_time: nil})
    {"+OK\r\n", new_state}
  end

  defp execute_command("*5$3SET", [_, _, _ | args], state) do
    [_, name, _, value, _, _, _, expiry_milliseconds] = args

    expire_time =
      DateTime.utc_now() |> DateTime.add(String.to_integer(expiry_milliseconds), :millisecond)

    new_state = Map.put(state, name, %{value: value, expire_time: expire_time})

    {"+OK\r\n", new_state}
  end

  defp execute_command("*2$3GET", [_, _, _, _, arg], state) do
    %{value: value, expire_time: expire_time} = Map.get(state, arg)

    if is_nil(expire_time) || expire_time >= DateTime.utc_now() do
      length = String.length(value)
      {"$#{length}\r\n#{value}\r\n", state}
    else
      {"$-1\r\n", state}
    end
  end
end
