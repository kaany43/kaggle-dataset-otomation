# Trendyol Süper Lig — Otonom Dataset Sistemi

Sofascore'dan gerçek zamanlı veri çeken, CSV olarak kaydeden ve **Kaggle'a otomatik yükleyen** tam otonom sistem.

---

## Dosya Yapısı

```
superlig-api/
├── main.py          ← FastAPI REST API
├── auto.py          ← Otonom dataset orchestrator  ⭐
├── collector.py     ← Sofascore veri çekme katmanı
├── writer.py        ← CSV + Kaggle metadata yazıcı
├── uploader.py      ← Kaggle API yükleme
├── export.py        ← Manuel/interaktif CSV export
├── scheduler.bat    ← Windows Task Scheduler kurulum
├── .env.example     ← Çevre değişkenleri şablonu
├── requirements.txt
└── data/
    └── 24-25/
        ├── standings.csv
        ├── teams.csv
        ├── team_stats.csv
        ├── matches.csv
        ├── goals.csv
        ├── cards.csv
        ├── substitutions.csv
        ├── match_stats.csv
        ├── player_profiles.csv
        ├── player_stats.csv
        └── dataset-metadata.json
```

---

## Kurulum

```bash
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env   # sonra .env'i düzenle
```

### Kaggle API Anahtarı
1. kaggle.com → Hesap → API → **Create New API Token**
2. `kaggle.json` dosyasını `C:\Users\<kullanıcı>\.kaggle\` klasörüne koy
3. **veya** `.env` dosyasına yaz:
```
KAGGLE_USERNAME=kullanici_adiniz
KAGGLE_KEY=api_anahtariniz
KAGGLE_DATASET_SLUG=superlig-full-dataset
```

---

## Kullanım

```bash
# Mevcut sezonu çek (Kaggle yok)
python auto.py

# Mevcut sezon + Kaggle'a yükle
python auto.py --upload

# Geçmiş sezon
python auto.py --season 2023

# Sezon ID listesi
python auto.py --list-seasons

# Sadece maç + olay verileri
python auto.py --modules matches events

# Sadece oyuncu verileri
python auto.py --modules players

# İlk kez Kaggle yükleme
python auto.py --upload --new
```

### Modüller

| Modül | Çıktı |
|---|---|
| `standings` | standings.csv |
| `teams` | teams.csv · team_stats.csv |
| `matches` | matches.csv |
| `events` | goals.csv · cards.csv · substitutions.csv |
| `match_stats` | match_stats.csv |
| `players` | player_profiles.csv · player_stats.csv |

---

## Otomatik Çalıştırma

**Windows:** `scheduler.bat`'ı Yönetici olarak çalıştırın → Her gece 03:00'de otomatik çalışır.

**Linux cron:**
```bash
0 3 * * * cd /proje && .venv/bin/python auto.py --upload >> logs/cron.log 2>&1
```

---

## Dataset Şeması

### winner_code
- `1` = Ev sahibi galip · `2` = Deplasman galip · `3` = Beraberlik

### goal_type
- `regular` · `penalty` · `ownGoal` · `freekick`

### player_stats.csv
50+ istatistik: gol, asist, xG, xA, pas %, dribling, top kazanma, hava topu, kurtarış, rating...

---

## Loglar
```
logs/run_history.json        ← Tüm çalışma geçmişi
logs/run_20250307_030000.log ← Detaylı log
```