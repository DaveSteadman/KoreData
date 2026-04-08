# MiniFeed

This is the requirements and top level design document for MiniFeed, an RSS ingest server for LLM Agents.
- Has an inventory of RSS feeds, organised by domain, including name, link and update rate.
- Maintains a SQLite database for each domain, containing the index number, feed name, page headline, metadata and page text
- Has a REST API that allows a user to manage the feed list as well as search for content.
- Allows a web user to navigate through the databases and browse all entries.


Application: 
- The server is at its core, a console application.

