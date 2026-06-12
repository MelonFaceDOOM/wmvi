# Podcast transcriber service

Downloads episode audio over HTTP, transcribes with Whisper, writes `podcasts.episodes` and segment rows. Runs on the **transcription** venv.

**No** YouTube cookies, proxy, or xrdp required.

## Run manually

From repo root:

```bash
python -m services.podcast.transcriber
python -m services.podcast.transcriber --prod
```

## Install (systemd)

```bash
sudo -E python -m services install podcast/transcriber
```

See [services/readme.md](../../readme.md).

## Verify

```bash
python transcription/transcription_checklist.py --group podcast
```

Requires DB episodes with a non-empty `download_url` (check downloads one row).

## Download behavior

- Resolves tracking URLs and tries a header ladder ([downloader.py](downloader.py)).
- Minimum output size 512 KB; smaller responses are treated as failed.
- Failures raise `DownloadFailed` with per-URL attempt reasons in logs.

## Export / import (prod pipeline)

Transcripts on gpu-pc are exported to nitwitch and imported on prod via unified content sync — see **Unified content sync** in [services/readme.md](../../readme.md).

Dev smoke test: [scripts/oneoffs/PODCAST_EXPORT_IMPORT_TEST.txt](../../../scripts/oneoffs/PODCAST_EXPORT_IMPORT_TEST.txt).

## Language labeling

`is_en` is set later by the `label_en` service (after transcript text is available), same as YouTube.

## Shared GPU setup

[transcription/setup.md](../../../transcription/setup.md) and [transcription/README.md](../../../transcription/README.md).
