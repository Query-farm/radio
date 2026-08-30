<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB Radio Extension

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/radio.html)
[![v1.5 build](https://github.com/Query-farm/radio/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/radio/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

The **Radio** extension by **Query.Farm** enables DuckDB to interact seamlessly with real-time event systems such as WebSocket servers, message queues, and event buses. It allows DuckDB to both **receive** and **send** events: incoming messages are buffered and queryable with standard SQL, while outgoing events are also buffered and support delivery tracking.

The extension is named *Radio* because it effectively equips DuckDB with a two-way radio—allowing it to **listen for** and **broadcast** messages across event-driven systems.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/radio](https://query.farm/products/extensions/radio)**

## Installation

```sql
INSTALL radio FROM community;
LOAD radio;
```

## Development

For instructions on building the extension from source and running its tests, see [BUILDING.md](BUILDING.md).
