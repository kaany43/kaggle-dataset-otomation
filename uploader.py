import logging
import os
import subprocess
from pathlib import Path

log = logging.getLogger("uploader")


def kaggle_configured() -> bool:
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return True

    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    return kaggle_json.exists()


def upload_dataset(
    dataset_dir: str,
    username: str,
    slug: str,
    version_notes: str = "Automated update",
    new_dataset: bool = False,
) -> bool:
    if not kaggle_configured():
        log.error(
            "Kaggle credentials are missing. Configure KAGGLE_USERNAME/KAGGLE_KEY "
            "or place kaggle.json in ~/.kaggle/."
        )
        return False

    try:
        import kaggle  # noqa: F401
    except ImportError:
        log.error("The 'kaggle' package is not installed.")
        return False

    if new_dataset:
        command = ["kaggle", "datasets", "create", "-p", dataset_dir, "--dir-mode", "zip"]
        log.info("Creating new Kaggle dataset: %s/%s", username, slug)
    else:
        command = [
            "kaggle",
            "datasets",
            "version",
            "-p",
            dataset_dir,
            "-m",
            version_notes,
            "--dir-mode",
            "zip",
        ]
        log.info("Creating dataset version for %s/%s", username, slug)

    result = subprocess.run(command, capture_output=True, text=True, check=False)

    if result.returncode == 0:
        if result.stdout.strip():
            log.info(result.stdout.strip())
        log.info("Kaggle upload completed successfully.")
        return True

    log.error("Kaggle upload failed with code %s.", result.returncode)
    if result.stderr.strip():
        log.error(result.stderr.strip())
    return False
