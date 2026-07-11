# Project Overview

## What This Is

A Java application that consumes Wikimedia recent-change events via Server-Sent Events (SSE) and publishes them to Kafka.

## Goals

- Stream real-time Wikimedia change events (recentchanges EventStream) into Kafka topics
- Learning project for Kafka producer/consumer patterns and SSE event sourcing

## Scope

- SSE consumer connecting to Wikimedia EventStream API
- Kafka producer writing change events to a topic
- Single-process command-line application (no UI, no web server)

## Tech Stack

- Java 17, Maven
- okhttp-eventsource for SSE
- kafka-clients for Kafka producer/consumer
