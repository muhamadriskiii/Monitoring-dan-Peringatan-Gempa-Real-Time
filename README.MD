# Monitoring dan Peringatan Gempa Real-Time

Pipeline ETL otomatis yang memantau gempa bumi terkini dari BMKG, menyimpan
datanya ke PostgreSQL, dan mengirim notifikasi Telegram dalam waktu WITA.

Seluruh komponen sudah berjalan di VPS:

- **Apache Airflow 3.3.2** berjalan di Docker dan menjalankan pipeline tiap 1 menit.
- **PostgreSQL 16 terpasang native di VPS** (bukan container).
- **Source project ini** di-mount ke container Airflow pada
  `/airflow/pipeline_gempa`.

Panduan ini mengasumsikan seluruh komponen berjalan di VPS, bukan di komputer
lokal Windows.

## Status live (per 25 Nov 2026)

- Airflow sehat: apiserver, scheduler, worker, triggerer, dag-processor + redis:7.2 (broker), compose project `airflow_docker`.
- DAG `gempa_idn` jalan tiap 1 menit.
- Channel Telegram INFO GEMPA IDN (https://t.me/infgmpidn), bot @infogmp_bot admin.
- Catatan: restart Airflow masih diperlukan agar DAG memakai `TELEGRAM_CHAT_ID`
  channel — saat ini notifikasi masih mengarah ke DM pribadi.

## Arsitektur

```text
VPS
├── Docker (compose project: airflow_docker)
│   ├── Container Airflow 3.3.2
│   │   ├── apiserver, scheduler, worker, triggerer, dag-processor
│   │   ├── DAG gempa_idn (jadwal tiap 1 menit)
│   │   └── /airflow/pipeline_gempa  ← source project di-mount ke sini
│   └── redis:7.2 (broker)
│
├── PostgreSQL 16 (native, di luar Docker)
│   └── database infogempa_id → tabel gempa_idn
│       (container Airflow terhubung lewat host.docker.internal)
│
├── akses keluar HTTPS → API BMKG
└── akses keluar HTTPS → Telegram Bot API
```

PostgreSQL terpasang native di OS VPS (bukan container), jadi dari container
Airflow koneksi memakai `host.docker.internal` (dengan
`extra_hosts: host-gateway` di `docker-compose.yaml`) atau IP VPS — bukan
`localhost` dan bukan nama service `postgres`.

## Alur Pipeline

```text
extract → raw.json → transform → clean.csv → load → PostgreSQL + notif.json → notify → Telegram
```

| Tahap     | Script             | Input             | Output                                |
|-----------|--------------------|-------------------|---------------------------------------|
| Extract   | `src/extract.py`   | API BMKG          | `data/raw.json`                       |
| Transform | `src/transform.py` | `data/raw.json`   | `data/clean.csv`                      |
| Load      | `src/load.py`      | `data/clean.csv`  | Tabel `gempa_idn` + `data/notif.json` |
| Notify    | `src/notify.py`    | `data/notif.json` | Pesan Telegram (WITA)                 |

Detail tiap tahap:

- **Extract** — mengambil `Infogempa.gempa` dari
  `https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json` dengan timeout
  20 detik, lalu menulis array hasilnya ke `data/raw.json`.
- **Transform** — membaca `data/raw.json`, mengganti nama kolom
  `DateTime` → `datetime_utc`, mengubah semua nama kolom dan nilai teks
  menjadi huruf kecil, lalu menghapus duplikat berdasarkan `datetime_utc`.
  Hasilnya ditulis ke `data/clean.csv`; DataFrame dan info-nya juga dicetak
  ke log task.
- **Load** — membaca `data/clean.csv` dan memasukkan 8 kolom ke tabel
  `gempa_idn` dengan
  `ON CONFLICT (datetime_utc) DO NOTHING RETURNING datetime_utc`. Hanya baris
  yang benar-benar baru yang ditulis ke `data/notif.json` dengan field
  `datetime_utc`, `magnitude`, `kedalaman`, `wilayah`, dan `potensi`.
  Kolom `lintang` dan `bujur` tetap ada di CSV tetapi tidak dimuat ke database.
- **Notify** — membaca `data/notif.json`, mengonversi `datetime_utc` (UTC) ke
  WITA (UTC+8), meng-escape HTML, lalu mengirim satu pesan per data. Pesan
  berisi waktu WITA, magnitudo, kedalaman, wilayah, potensi, dan link sumber
  BMKG. Error pada satu data tidak menghentikan proses data berikutnya.

## Teknologi

- Python 3 — `requests`, `pandas`, `psycopg2`, `python-dotenv`
- PostgreSQL 16 native di VPS (bukan container)
- Apache Airflow 3.3.2 (Docker di VPS) — DAG `gempa_idn`, jadwal tiap 1 menit
- Telegram Bot API — notifikasi
- Docker Compose — menjalankan Airflow (compose project `airflow_docker`)

## Struktur Folder

```text
pipeline_gempa/
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── notify.py
│   └── db_config.py
├── dags/
│   └── gempa_dag.py
├── data/                  # dibuat manual, isinya digenerate pipeline
│   ├── raw.json
│   ├── clean.csv
│   └── notif.json
├── schema.sql
├── .env                   # tidak di-commit
├── .env.example
└── README.MD
```

Tidak ada `requirements.txt`; dependensi dipasang di image/container Airflow.

## Konfigurasi (`.env`)

```ini
DB_PASSWORD=password_postgres
TELEGRAM_TOKEN=123456:token_bot
TELEGRAM_CHAT_ID=123456789
GEMPA_DB_HOST=host.docker.internal
```

| Variabel           | Kegunaan                                        | Default              |
|--------------------|-------------------------------------------------|----------------------|
| `DB_PASSWORD`      | Password PostgreSQL untuk user `riski`          | wajib diisi          |
| `TELEGRAM_TOKEN`   | Token bot Telegram                              | wajib diisi          |
| `TELEGRAM_CHAT_ID` | ID chat tujuan notifikasi                       | wajib diisi          |
| `GEMPA_DB_HOST`    | Host PostgreSQL yang dilihat proses pipeline    | `host.docker.internal` |

Nama database (`infogempa_id`) dan user (`riski`) di-hardcode di
`src/db_config.py`. Password dibaca dari `.env`; `.env` dimuat secara eksplisit
dari root project oleh `db_config.py`.

- Dari container Airflow, isi `GEMPA_DB_HOST` dengan `host.docker.internal`
  dan tambahkan `extra_hosts: - "host.docker.internal:host-gateway"` pada
  service Airflow di `docker-compose.yaml`. Alternatifnya, isi dengan IP VPS.
  `localhost` akan menunjuk ke container Airflow sendiri sehingga koneksi gagal.
- Jangan memakai nama `DB_HOST` untuk variabel ini — nama tersebut dipakai
  internal oleh Airflow dan dapat menyebabkan scheduler crash-loop.
- Variabel juga bisa diberikan lewat `environment`/`env_file` di
  `docker-compose.yaml`; nilai environment container menang atas isi `.env`.

> **Penting:** `schema.sql` saat ini masih membuat tabel `gempa_bmkg`, sedangkan
> `src/load.py` memasukkan data ke tabel `gempa_idn`. Samakan nama tabel di
> salah satu file sebelum membuat tabel, misalnya ubah `schema.sql` menjadi
> `CREATE TABLE gempa_idn (...)`.

## Deployment di VPS

### 1. Prasyarat

- VPS Linux dengan Docker Engine dan Docker Compose plugin.
- PostgreSQL 16 native sudah terpasang di VPS dan dapat diakses dari container
  Airflow lewat `host.docker.internal` (`extra_hosts: host-gateway`).
- Port Airflow UI (umumnya `8080`) dibuka seperlunya; port PostgreSQL `5432`
  tidak perlu diekspos ke publik.
- VPS dapat mengakses HTTPS ke `data.bmkg.go.id` dan `api.telegram.org`.

### 2. Siapkan project dan folder data

Kirim folder project dari laptop (PowerShell baru, bukan dari dalam sesi SSH):

```powershell
scp -r D:\pipeline_gempa muhamdriss@20.17.105.213:~/pipeline_gempa_idn
```

Lalu di VPS:

```bash
cd ~/pipeline_gempa_idn
mkdir -p data
cp .env.example .env
nano .env
```

`extract.py` menulis ke `data/raw.json`, sehingga folder `data/` harus sudah
ada. Script tidak membuat folder tersebut secara otomatis.

### 3. Siapkan PostgreSQL

PostgreSQL native di VPS (bukan container). Buat database dan user sesuai
`src/db_config.py`:

```sql
CREATE USER riski WITH PASSWORD 'password_postgres';
CREATE DATABASE infogempa_id OWNER riski;
```

Lalu buat tabel `gempa_idn`:

```sql
\c infogempa_id

CREATE TABLE gempa_idn (
    datetime_utc TIMESTAMPTZ PRIMARY KEY,
    tanggal      VARCHAR(20),
    jam          VARCHAR(20),
    coordinates  VARCHAR(30),
    magnitude    FLOAT,
    kedalaman    VARCHAR(20),
    wilayah      TEXT,
    potensi      VARCHAR(50),
    tanggal_ditambahkan TIMESTAMPTZ DEFAULT now()
);
```

Kolom `tanggal_ditambahkan` terisi otomatis dengan jam baris masuk ke database
(`datetime_utc` = jam gempanya terjadi).

Jika memakai `schema.sql`, ubah dulu nama tabelnya dari `gempa_bmkg` menjadi
`gempa_idn`, lalu jalankan dengan user `riski`:

```bash
psql -h <host> -U riski -d infogempa_id -f schema.sql
```

### 4. Konfigurasi Docker Compose Airflow

Sesuaikan `docker-compose.yaml` (misalnya compose resmi Airflow) dengan
kebutuhan project:

- Mount source project ke path yang dipakai DAG:
  `- /home/muhmdriss/pipeline_gempa_idn:/airflow/pipeline_gempa`
- Mount folder DAG agar terbaca Airflow:
  `- /home/muhmdriss/pipeline_gempa_idn/dags:/opt/airflow/dags`
  (atau copy `dags/gempa_dag.py` ke folder `dags` Airflow)
- Set host database di environment service Airflow:
  `GEMPA_DB_HOST: host.docker.internal` (PostgreSQL native)
- Tambahkan pada service Airflow:
  ```yaml
  extra_hosts:
    - "host.docker.internal:host-gateway"
  ```
- Pastikan dependensi tersedia di image Airflow, misalnya:

  ```yaml
  environment:
    _PIP_ADDITIONAL_REQUIREMENTS: "pandas requests psycopg2-binary python-dotenv"
  ```

- Jika `.env` ikut ter-mount bersama project, `db_config.py` dan `notify.py`
  dapat membacanya langsung. Alternatifnya, set `DB_PASSWORD`,
  `TELEGRAM_TOKEN`, dan `TELEGRAM_CHAT_ID` sebagai environment container.

DAG memakai `SRC = "/airflow/pipeline_gempa/src"` sehingga source project harus
tersedia tepat di path tersebut. Jika lokasi project di VPS berbeda, ubah
`SRC` di `dags/gempa_dag.py` atau sesuaikan target mount-nya.

### 5. Jalankan Airflow

```bash
docker compose up -d
docker compose ps
```

Gunakan `up -d` (bukan `restart`) setelah mengubah `docker-compose.yaml`,
karena `restart` tidak membaca ulang konfigurasi YAML. Tunggu service berstatus
`(healthy)`, lalu buka `http://IP_VPS:8080` dan login sesuai kredensial yang
diset di compose (contoh compose resmi: `airflow` / `airflow`).

Aktifkan (unpause) DAG `gempa_idn`. Pipeline akan berjalan otomatis setiap
1 menit.

### 6. Uji manual (opsional)

Dari dalam container Airflow:

```bash
docker compose exec airflow-scheduler bash
cd /airflow/pipeline_gempa
python src/extract.py
python src/transform.py
python src/load.py
python src/notify.py
```

Jalankan berurutan karena tiap tahap membaca output tahap sebelumnya. Cara lain
adalah memicu DAG secara manual dari UI Airflow.

## DAG

| Properti      | Nilai                                      |
|---------------|--------------------------------------------|
| `dag_id`      | `gempa_idn`                                |
| Schedule      | `*/1 * * * *` (tiap 1 menit)               |
| `start_date`  | `2026-10-08`                               |
| `catchup`     | `False`                                    |
| Retry         | 1 kali, delay 1 menit                      |
| Tasks         | `extract >> transform >> load >> notify`   |
| Source di container | `/airflow/pipeline_gempa/src`        |

## Catatan Desain

- Tidak ada filter wilayah — semua gempa yang tercatat BMKG masuk ke database
  dan Telegram. Pada run pertama setelah deploy, semua gempa yang sedang ada di
  feed BMKG dianggap data baru sehingga Telegram dapat menerima banyak pesan
  sekaligus (wajar, hanya terjadi sekali).
- `Coordinates` disimpan sebagai teks (tidak dipecah jadi latitude/longitude) —
  cukup untuk arsip dan notifikasi. `lintang` dan `bujur` tidak dimuat ke
  database dan tidak dipakai di pesan Telegram.
- Upsert memakai `ON CONFLICT (datetime_utc) DO NOTHING` — data duplikat tidak
  masuk dua kali.
- `notif.json` hanya berisi data yang benar-benar baru; kalau kosong, Telegram
  tidak mengirim apa pun (tidak ada notifikasi ganda).
- Waktu di database disimpan sebagai `TIMESTAMPTZ` (UTC), sedangkan pesan
  Telegram menampilkan WITA (`UTC+8`) melalui fungsi `utc_to_wita`.
- Nilai dari JSON di-escape dengan `html.escape` sebelum dikirim agar karakter
  `<`, `>`, dan `&` tidak merusak format HTML Telegram.
- `notify.py` menangkap exception per data, tetapi belum memeriksa status HTTP
  respons Telegram. Angka `terkirim` di log berarti data yang diproses tanpa
  exception, bukan jaminan pesan diterima Telegram.
- Semua script memakai pola portable agar path tidak bergantung folder kerja:
  ```python
  from pathlib import Path
  BASE = Path(__file__).resolve().parent.parent
  ```
  Jangan memakai path absolut (`D:/...`) atau path relatif (`data/...`) di dalam
  script.

## Troubleshooting

| Gejala | Penyebab | Solusi |
|--------|----------|--------|
| DAG tidak muncul di UI | Folder `dags` tidak ter-mount / scheduler belum sehat | Mount `dags` ke container, cek `docker compose ps`, jalankan `airflow dags list` |
| Task "No Status" terus | Scheduler/worker belum healthy | Tunggu status `(healthy)` di `docker compose ps` |
| Scheduler restart berulang | Env `DB_HOST` bentrok dengan internal Airflow | Ganti menjadi `GEMPA_DB_HOST` |
| Perubahan YAML/`.env` tidak terbaca | `docker compose restart` tidak memuat ulang config | Jalankan `docker compose up -d` (tambahkan `--force-recreate` bila perlu) |
| `could not connect to server` / connection refused | `GEMPA_DB_HOST` masih `localhost` dari dalam container | Isi dengan `host.docker.internal` (pastikan `extra_hosts: host-gateway` ada di compose) atau IP VPS |
| `relation "gempa_idn" does not exist` | `schema.sql` membuat tabel `gempa_bmkg` | Buat tabel `gempa_idn` atau samakan nama di `schema.sql`/`load.py` |
| PostgreSQL Docker dan native bentrok di port 5432 | Dua PostgreSQL tidak boleh jalan barengan di port yang sama | Jangan jalankan PostgreSQL container — pakai yang native saja |
| `FileNotFoundError` pada `data/*.json` atau `*.csv` | Folder `data/` belum ada atau task sebelumnya gagal | Jalankan `mkdir -p data`, cek log task sebelumnya, jalankan task berurutan |
| Tidak ada pesan Telegram | `notif.json` kosong (tidak ada gempa baru) atau kredensial salah | Cek log task `load`/`notify`, isi `.env`, dan `TELEGRAM_CHAT_ID` |
| UI Airflow tidak bisa dibuka | Port belum dipublish atau diblokir firewall VPS | Cek mapping port di compose dan aturan firewall/security group |
