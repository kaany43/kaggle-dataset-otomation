"""
uploader.py — Kaggle API ile dataset yükleme / güncelleme

Gereksinimler:
    pip install kaggle

Kaggle API kimlik bilgileri:
    1. kaggle.com → Account → API → "Create New API Token"
    2. İndirilen kaggle.json dosyasını kopyalayın:
       Windows : C:\\Users\\<kullanıcı>\\.kaggle\\kaggle.json
       Linux   : ~/.kaggle/kaggle.json
    3. Ya da .env dosyasına yazın:
       KAGGLE_USERNAME=kullanıcı_adınız
       KAGGLE_KEY=api_anahtarınız
"""

import os
import sys
import logging
import subprocess

log = logging.getLogger("uploader")


def _kaggle_configured() -> bool:
    """kaggle.json veya env var mevcut mu?"""
    # Env var
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return True
    # kaggle.json
    home = os.path.expanduser("~")
    path = os.path.join(home, ".kaggle", "kaggle.json")
    return os.path.exists(path)


def upload_dataset(
    dataset_dir: str,
    username: str,
    slug: str,
    version_notes: str = "Auto-update",
    new_dataset: bool = False,
) -> bool:
    """
    dataset_dir   : CSV'lerin ve dataset-metadata.json'ın olduğu klasör
    username      : Kaggle kullanıcı adı
    slug          : Dataset slug (örn: superlig-full-dataset)
    version_notes : Sürüm notu
    new_dataset   : True → yeni dataset oluştur, False → mevcut güncelle
    """
    if not _kaggle_configured():
        log.error(
            "Kaggle kimlik bilgileri bulunamadı!\n"
            "  → ~/.kaggle/kaggle.json oluşturun veya\n"
            "  → KAGGLE_USERNAME / KAGGLE_KEY env değişkenlerini ayarlayın"
        )
        return False

    try:
        import kaggle  # noqa: F401 — sadece kurulu mu diye kontrol
    except ImportError:
        log.error("kaggle paketi bulunamadı. Kurun: pip install kaggle")
        return False

    if new_dataset:
        cmd = ["kaggle", "datasets", "create", "-p", dataset_dir, "--dir-mode", "zip"]
        log.info(f"Yeni dataset oluşturuluyor: {username}/{slug}")
    else:
        cmd = [
            "kaggle", "datasets", "version",
            "-p", dataset_dir,
            "-m", version_notes,
            "--dir-mode", "zip",
        ]
        log.info(f"Dataset güncelleniyor: {username}/{slug}  → \"{version_notes}\"")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        log.info(f"Kaggle yükleme başarılı!\n{result.stdout.strip()}")
        return True
    else:
        log.error(f"Kaggle yükleme hatası:\n{result.stderr.strip()}")
        return False


def check_dataset_exists(username: str, slug: str) -> bool:
    """Dataset zaten Kaggle'da var mı?"""
    cmd = ["kaggle", "datasets", "status", f"{username}/{slug}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0