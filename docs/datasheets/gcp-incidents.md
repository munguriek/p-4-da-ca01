# Google Cloud Service Health incident history
Publisher and URL: Google, https://status.cloud.google.com/incidents.json, described by https://status.cloud.google.com/incidents.schema.json
Licence or terms: Google's terms; no licence is stated for the feed (to confirm)
How it arrives (stream, poll or file) and how often: file, downloaded once
Format and version: a JSON array of incidents with nested updates, products and locations; the feed has no version, so the schema file is kept beside it
Personal data, and how it is handled: none
Known issues: the file is live, so each download differs and only this snapshot's checksum can match; Google treats id fields as stable and display names as liable to change, so joins use ids
Checksum and download date (files): incidents.json, SHA-256 TODO; incidents.schema.json, SHA-256 TODO; downloaded TODO
Registered on: TODO
