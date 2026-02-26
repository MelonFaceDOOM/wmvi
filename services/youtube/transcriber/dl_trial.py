from services.youtube.transcriber.youtube_transcriber import download_yt_audio

url = "https://www.youtube.com/watch?v=Zti7B5dOdao"
download_yt_audio(url, "yt_audio_test.mp3")
