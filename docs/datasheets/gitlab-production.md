# GitLab.com production tracker
Publisher and URL: GitLab Inc., https://gitlab.com/gitlab-com/gl-infra/production; API https://gitlab.com/api/v4/projects/gitlab-com%2Fgl-infra%2Fproduction/issues
Licence or terms: GitLab.com terms; the issues are public and no licence is stated (to confirm)
How it arrives (stream, poll or file) and how often: poll, every 10 minutes from week 2, with updated_after at the watermark minus 15 minutes
Format and version: JSON from the GitLab REST API v4
Personal data, and how it is handled: GitLab users' usernames, display names and @-mentions, also in label events; the poller replaces them with HMAC-SHA256 pseudonyms keyed from .env and drops web_url and avatar_url before anything reaches Kafka, as the README's Ethics section promises
Known issues: the read_api token expires on TODO; renew it before then
Checksum and download date (files): not a file
Registered on: TODO
