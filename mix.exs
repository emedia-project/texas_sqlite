defmodule Texas.Sqlite.Mixfile do
  use Mix.Project

  def project do
    [
      app: :texas_sqlite,
      version: "0.0.1",
      elixir: "~> 1.2",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps
    ]
  end

  def application do
    [
       applications: [],
       env: []
    ]
  end

  defp deps do
    [
      {:lager, "~> 3.2"},
      {:texas_adapter, git: "https://github.com/emedia-project/texas_adapter.git", branch: "master"},
      {:esqlite, "~> 0.2.2"}    
    ]
  end
end