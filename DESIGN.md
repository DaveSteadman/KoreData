# KoreData

## Purpose
A project to capture almost any form of text data for presentation to an LLM agent.

## Top level architecture:
This is a project to contain a number of data services:
- KoreFeed: A source of active published data and editorial content, from RSS feeds and websites. The data is considered short term and the source is part of the data.
- KoreReference: An encylopedia of interlinked data (A wikipedia clone).
- KoreLibrary: Long form static and unlinked data such as ebooks (Project Guttenberg clone).
- KoreRAG: A user-managed store of arbitrary text chunks intended for retrieval-augmented generation (RAG). Each chunk carries a title, source URL, freeform tags, and compressed body text. Content is inserted manually or via API.

A top level KoreDataGateway application is a single point of contact for an agent and web-ui to interface to the data to add, manage and search the content.



