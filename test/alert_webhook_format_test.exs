defmodule TimelessMetrics.AlertWebhookFormatTest do
  @moduledoc """
  Covers the ntfy payload translation.

  ntfy parses a JSON body only at its root endpoint. Posted to a topic path, the
  entire envelope becomes the message text, so alerts arrive as an unreadable
  blob with no title or priority. These tests pin the translation that avoids it.
  """
  use ExUnit.Case, async: true

  alias TimelessMetrics.Alert

  defp rule(overrides \\ %{}) do
    Map.merge(
      %{
        name: "High CPU",
        metric: "cpu_usage",
        threshold: 90.0,
        condition: "above",
        aggregate: "avg",
        webhook_url: "https://ntfy.sh/my-alerts",
        webhook_format: "ntfy"
      },
      overrides
    )
  end

  describe "ntfy format" do
    test "posts to the root endpoint, not the topic path" do
      {url, body} =
        Alert.build_delivery(rule(), %{"host" => "web-1"}, 94.2, "firing", 123, "chart?x=1")

      assert url == "https://ntfy.sh/"
      assert body.topic == "my-alerts"
    end

    test "produces a readable message rather than a JSON blob" do
      {_url, body} =
        Alert.build_delivery(rule(), %{"host" => "web-1"}, 94.2, "firing", 123, "chart?x=1")

      assert body.title == "FIRING: High CPU"
      assert body.message =~ "cpu_usage"
      assert body.message =~ "host=web-1"
      assert body.message =~ "94.2"
      assert body.message =~ "above"
      refute body.message =~ "{\"alert\""
    end

    test "firing is louder than resolved" do
      {_url, firing} =
        Alert.build_delivery(rule(), %{}, 94.2, "firing", 123, "chart?x=1")

      {_url, resolved} =
        Alert.build_delivery(rule(), %{}, 10.0, "resolved", 123, "chart?x=1")

      assert firing.priority > resolved.priority
      assert firing.tags == ["rotating_light"]
      assert resolved.tags == ["white_check_mark"]
      assert resolved.title == "RESOLVED: High CPU"
    end

    test "handles a rule with no labels" do
      {_url, body} = Alert.build_delivery(rule(), %{}, 94.2, "firing", 123, "chart?x=1")

      assert body.message =~ "cpu_usage is 94.2"
    end

    test "supports a self-hosted instance under a path prefix" do
      r = rule(%{webhook_url: "https://ntfy.example.com/ops/noc-alerts"})
      {url, body} = Alert.build_delivery(r, %{}, 1.0, "firing", 123, "chart")

      assert url == "https://ntfy.example.com/ops/"
      assert body.topic == "noc-alerts"
    end
  end

  describe "default format" do
    test "is unchanged — same URL and the original envelope" do
      r = rule(%{webhook_format: nil, webhook_url: "http://noc-bot:3000/webhook"})

      {url, body} =
        Alert.build_delivery(r, %{"host" => "web-1"}, 94.2, "firing", 123, "chart?x=1")

      assert url == "http://noc-bot:3000/webhook"
      assert body.alert == "High CPU"
      assert body.metric == "cpu_usage"
      assert body.state == "firing"
      assert body.triggered_at == 123
      assert body.url == "chart?x=1"
      refute Map.has_key?(body, :topic)
    end
  end

  describe "split_ntfy_url/1" do
    test "splits a plain topic URL" do
      assert {"https://ntfy.sh/", "my-topic"} = Alert.split_ntfy_url("https://ntfy.sh/my-topic")
    end

    test "preserves a non-default port" do
      assert {"http://localhost:8080/", "t"} = Alert.split_ntfy_url("http://localhost:8080/t")
    end

    test "drops any query string" do
      assert {"https://ntfy.sh/", "t"} = Alert.split_ntfy_url("https://ntfy.sh/t?foo=bar")
    end
  end
end
