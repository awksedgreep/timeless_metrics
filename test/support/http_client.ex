defmodule TimelessMetrics.TestHTTP do
  @moduledoc """
  Lightweight HTTP client for testing TimelessMetrics.HTTP endpoints.
  Uses :httpc (built-in) to make real HTTP requests to a running Rocket server.
  """

  @connect_timeout 5_000
  @recv_timeout 30_000

  def get(port, path, opts \\ []) do
    request(:get, port, path, nil, opts)
  end

  def post(port, path, body, opts \\ []) do
    result = request(:post, port, path, body, opts)

    # Wait for async ingest queue to process after import POSTs
    if String.contains?(path, "/import") do
      Process.sleep(50)
    end

    result
  end

  def put(port, path, body, opts \\ []) do
    request(:put, port, path, body, opts)
  end

  def delete(port, path, opts \\ []) do
    request(:delete, port, path, nil, opts)
  end

  defp request(method, port, path, body, opts) do
    headers = Keyword.get(opts, :headers, [])
    content_type = Keyword.get(opts, :content_type)

    # :httpc requires certain chars to be percent-encoded
    safe_path =
      path
      |> String.replace("|", "%7C")
      |> String.replace("[", "%5B")
      |> String.replace("]", "%5D")

    url = ~c"http://127.0.0.1:#{port}#{safe_path}"

    http_opts = [
      timeout: @recv_timeout,
      connect_timeout: @connect_timeout
    ]

    req_opts = [body_format: :binary]

    result =
      case method do
        :get ->
          header_list = to_httpc_headers(headers)
          :httpc.request(:get, {url, header_list}, http_opts, req_opts)

        :delete ->
          header_list = to_httpc_headers(headers)
          :httpc.request(:delete, {url, header_list}, http_opts, req_opts)

        method when method in [:post, :put] ->
          ct = to_charlist(content_type || "application/octet-stream")
          header_list = to_httpc_headers(headers)
          body_bin = body || ""
          :httpc.request(method, {url, header_list, ct, body_bin}, http_opts, req_opts)
      end

    case result do
      {:ok, {{_http_ver, status, _reason}, resp_headers, resp_body}} ->
        %{
          status: status,
          body: to_string(resp_body),
          headers: normalize_headers(resp_headers)
        }

      {:error, reason} ->
        raise "HTTP request failed: #{inspect(reason)}"
    end
  end

  defp to_httpc_headers(headers) do
    Enum.map(headers, fn {k, v} -> {to_charlist(k), to_charlist(v)} end)
  end

  defp normalize_headers(headers) do
    Enum.map(headers, fn {k, v} -> {to_string(k), to_string(v)} end)
  end
end
