# WMVI (Web Monitoring for Vaccine Information)
Collect social-media posts about vaccines, normalize them, and track term-level matches for analysis.


## Database
- Platform-specific tables store raw ingested content (sm.*, podcasts.*, news.*).
- All posts are unified through a single registry table (sm.post_registry).
- Views provide common surfaces for search and processing (e.g. full-text search).

## Scripts

There are various dev tool scripts meant to be run as-needed. All scripts are run via `python -m` from the project root. 

## Services

There are a combination of periodic and continuous services. They are designed to be run using systemd and there are template files for each service.

## Virtual Environments
WMVI intentionally uses two separate virtual environments.

Transcription services run in a dedicated virtual environment to avoid
installing heavy ML dependencies on machines that do not transcribe media.

It is recommended to install requirements.txt and requirements-transcription.txt to two separate venvs. The transcription folder has more info on setting up for transcription.
