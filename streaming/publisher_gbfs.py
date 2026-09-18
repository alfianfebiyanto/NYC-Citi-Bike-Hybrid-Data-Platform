from __future__ import annotations
import argparse
import json
import logging
import time
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound

from config.settings import (
    PROJECT_ID,
    GBFS_STATION_STATUS_URL,
    GBFS_POLL_INTERVAL_SECONDS,
    GBFS_HTTP_TIMEOUT_SECONDS,
    GBFS_TOPIC_ID,
    GBFS_SUBSCRIPTION_ID,
)

log = logging.getLogger("gbfs_producer")


# Pub/Sub Infrastructure
def ensure_pubsub_resources(publisher, subscriber) -> str:
    """Pastikan topic dan subscription GBFS tersedia."""

    # 1. Tentukan Pub/Sub topic dan subscription path.
    topic_path = publisher.topic_path(PROJECT_ID, GBFS_TOPIC_ID)
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, GBFS_SUBSCRIPTION_ID
    )

    try:
        publisher.get_topic(request={"topic": topic_path})
        log.info("Pub/Sub topic ready | topic=%s", GBFS_TOPIC_ID)
    except NotFound:
        try:
            publisher.create_topic(request={"name": topic_path})
            log.info("Pub/Sub topic created | topic=%s", GBFS_TOPIC_ID)
        except AlreadyExists:
            pass

    try:
        subscriber.get_subscription(request={"subscription": subscription_path})
        log.info(
            "Pub/Sub subscription ready | subscription=%s",
            GBFS_SUBSCRIPTION_ID,
        )
    except NotFound:
        try:
            subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
            log.info(
                "Pub/Sub subscription created | subscription=%s",
                GBFS_SUBSCRIPTION_ID,
            )
        except AlreadyExists:
            pass

    return topic_path


# GBFS Source
def fetch_station_status() -> dict:
    """Ambil station_status terbaru dari Citi Bike GBFS."""

    # 1. Ambil snapshot terbaru dari GBFS API.
    response = requests.get(
        GBFS_STATION_STATUS_URL,
        timeout=GBFS_HTTP_TIMEOUT_SECONDS,
        headers={"User-Agent": "citibike-pipeline/1.0"},
    )
    response.raise_for_status()
    return response.json()


def get_stations(payload: dict) -> list[dict]:
    """Ambil daftar station dari payload GBFS."""

    # 1. Ambil daftar station dari struktur payload GBFS.
    stations = payload.get("data", {}).get("stations")

    if not isinstance(stations, list):
        raise ValueError("GBFS data.stations tidak valid")

    return stations


# GBFS Publisher
def publish_snapshot(publisher, topic_path: str, payload: dict) -> tuple[int, int]:
    """Publish satu snapshot GBFS. Satu station = satu message."""

    # 1. Ambil seluruh station dari snapshot GBFS.
    stations = get_stations(payload)
    snapshot_timestamp = datetime.now(timezone.utc).isoformat()
    source_last_updated = payload.get("last_updated")

    futures = []
    failed = 0

    # 2. Siapkan dan publish setiap station ke Pub/Sub.
    for station in stations:
        station_id = station.get("station_id")

        if not station_id:
            failed += 1
            log.warning("Station tanpa station_id dilewati.")
            continue

        message = {
            **station,
            "source_last_updated": source_last_updated,
            "snapshot_timestamp": snapshot_timestamp,
        }

        try:
            future = publisher.publish(
                topic_path,
                json.dumps(message).encode("utf-8"),
                station_id=str(station_id),
            )
            futures.append((station_id, future))

        except Exception as exc:
            failed += 1
            log.error("Gagal submit station_id=%s | %s", station_id, exc)

    # 3. Pastikan seluruh asynchronous publish selesai.
    published = 0
    for station_id, future in futures:
        try:
            future.result(timeout=30)
            published += 1
        except Exception as exc:
            failed += 1
            log.error("Publish gagal station_id=%s | %s", station_id, exc)

    return published, failed


def poll_once(publisher, topic_path: str) -> None:
    """Jalankan satu polling snapshot GBFS."""

    try:
        # 1. Ambil snapshot terbaru dari GBFS API.
        payload = fetch_station_status()
        stations = get_stations(payload)
        published, failed = publish_snapshot(publisher, topic_path, payload)

        # 2. Catat hasil satu polling snapshot.
        log.info(
            "Snapshot completed | total=%d | published=%d | failed=%d",
            len(stations),
            published,
            failed,
        )

    except requests.RequestException as exc:
        log.error("Gagal mengambil GBFS | %s", exc)

    except Exception as exc:
        log.error("Snapshot gagal diproses | %s", exc)


# DLQ Test
def publish_dlq_test(publisher, topic_path: str, total: int) -> None:
    """Publish sejumlah invalid message untuk menguji DLQ."""

    # 1. Gunakan timestamp valid sebagai baseline test.
    now = datetime.now(timezone.utc).isoformat()

    # 2. Publish tiga variasi invalid event secara bergantian.
    for i in range(1, total + 1):
        error_type = (i - 1) % 3

        if error_type == 0:
            message = {
                "station_id": "",
                "num_bikes_available": 10,
                "snapshot_timestamp": now,
            }
            test_id = f"TEST-DLQ-EMPTY-{i}"

        elif error_type == 1:
            message = {
                "station_id": f"TEST-DLQ-NEGATIVE-{i}",
                "num_bikes_available": -5,
                "snapshot_timestamp": now,
            }
            test_id = message["station_id"]

        else:
            message = {
                "station_id": f"TEST-DLQ-TIMESTAMP-{i}",
                "num_bikes_available": 10,
                "snapshot_timestamp": "invalid-timestamp",
            }
            test_id = message["station_id"]

        publisher.publish(
            topic_path,
            json.dumps(message).encode("utf-8"),
            station_id=test_id,
        ).result(timeout=30)

    log.info("DLQ test completed | published=%d", total)


# Main
def main() -> None:
    """Jalankan GBFS publisher dalam normal atau DLQ test mode."""

    # 1. Siapkan command-line argument.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test-dlq",
        type=int,
        metavar="N",
        help="Publish N invalid messages untuk test DLQ lalu exit.",
    )
    args = parser.parse_args()

    # 2. Validasi jumlah message untuk DLQ test.
    if args.test_dlq is not None and args.test_dlq < 1:
        parser.error("--test-dlq harus minimal 1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    # 3. Inisialisasi Pub/Sub clients.
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = ensure_pubsub_resources(publisher, subscriber)

    # 4. DLQ test mode
    if args.test_dlq is not None:
        log.info("GBFS DLQ test started | total=%d", args.test_dlq)
        publish_dlq_test(publisher, topic_path, args.test_dlq)
        return

    # 5. Jalankan publisher dalam continuous streaming mode.
    log.info(
        "GBFS producer started | interval=%ss | topic=%s",
        GBFS_POLL_INTERVAL_SECONDS,
        GBFS_TOPIC_ID,
    )

    try:
        while True:
            started = time.monotonic()
            poll_once(publisher, topic_path)

            elapsed = time.monotonic() - started
            sleep_seconds = max(1, GBFS_POLL_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_seconds)

    # 8. Hentikan publisher dengan aman saat menerima interrupt ( CTRL +C )
    except KeyboardInterrupt:
        log.info("GBFS producer stopped.")

if __name__ == "__main__":
    main()