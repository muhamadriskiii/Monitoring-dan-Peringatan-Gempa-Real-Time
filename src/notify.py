import json, os, requests, html
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
load_dotenv()

def utc_to_wita(utc_dt):
    wita = timezone(timedelta(hours=8))
    if isinstance(utc_dt, str):
        utc_dt = datetime.fromisoformat(utc_dt.replace("Z", "+00:00"))
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(wita)

def kirim_pesan(teks):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("TELEGRAM_TOKEN / TELEGRAM_CHAT_ID tidak ada di .env")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, data={"chat_id": chat_id, "text": teks, "parse_mode": "html"}, timeout=20)
    except Exception as e:
        print(f"Gagal kirim pesan: {e}")

with open(BASE / "data" / "notif.json", encoding="utf-8") as f:
    daftar = json.load(f)

terkirim = 0
for g in daftar:
    try:
        wita = utc_to_wita(g.get("datetime_utc"))
        teks = (
            f"<b>INFORMASI GEMPA\n</b>"
            f"Waktu: {wita.strftime('%Y-%m-%d %H:%M WITA')}\n"
            f"Magnitudo: {html.escape(str(g.get('magnitude', '-')))}\n"
            f"Kedalaman: {html.escape(str(g.get('kedalaman', '-')))}\n"
            f"Wilayah: {html.escape(str(g.get('wilayah', '-')))}\n"
            f"Potensi: {html.escape(str(g.get('potensi', '-')))}\n"
            f"Sumber: https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json"
        )
        kirim_pesan(teks)
        terkirim += 1
    except Exception as e:
        print(f"Gagal proses data: {e}")

print(f"{terkirim}/{len(daftar)} notifikasi terkirim")