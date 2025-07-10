import time
import streamlit as st
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from services.llm_pipeline.r1_utils import process_row


def resume_round_1(work_df, sfw_df, ckpt, progress_bar=None, caption=None):
    """
    Batch-process Round 1 prompts with:
      - 10 workers
      - Sleep 10s every 40 calls
      - Checkpoint every 30
      - Show tqdm progress bar and Streamlit progress
      - Display estimated time remaining
    """
    client = None

    # pull pending + results from checkpoint
    pending = ckpt.state["r1_pending"][:]  # list of idxs
    results = ckpt.state["r1_results"][:]  # list of already-done

    total = len(pending) + len(results)  # how many in total

    # Time estimation parameters based on experimentation
    ROUND1_RATE = 1.79  # rows per second

    pbar = tqdm(
        total=total, initial=len(results), desc="Round1 rows processed", unit="row"
    )

    skill_info, lock = {}, Lock()
    api_calls = len(results)
    processed = len(results)

    # Track start time for better estimation
    start_time = time.time()

    def update_caption_with_eta(processed, total):
        if caption is not None:
            remaining = total - processed
            if processed > 0:
                # Calculate dynamic rate based on actual progress
                elapsed = time.time() - start_time
                actual_rate = processed / elapsed if elapsed > 0 else ROUND1_RATE
                # Use a blend of actual rate and expected rate for stability
                estimated_rate = (actual_rate + ROUND1_RATE) / 2
            else:
                estimated_rate = ROUND1_RATE

            eta_seconds = remaining / estimated_rate if estimated_rate > 0 else 0

            if eta_seconds > 3600:  # More than 1 hour
                eta_minutes = int(
                    (eta_seconds / 60) + 0.5
                )  # Round up to nearest minute
                eta_str = f"ETA {eta_minutes} minutes"
            elif eta_seconds > 60:  # More than 1 minute
                eta_minutes = int(
                    (eta_seconds / 60) + 0.5
                )  # Round up to nearest minute
                eta_str = f"ETA {eta_minutes} minutes"
            else:
                eta_str = "ETA less than 1 minute"

            caption.caption(f"[Status] Processing 1st Stage... ({eta_str})")

    # Initial caption update
    update_caption_with_eta(processed, total)

    while pending:

        batch = pending[:10]
        pending = pending[10:]
        rows = work_df.loc[batch]

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    process_row, rows.loc[idx], skill_info, sfw_df, lock
                ): idx
                for idx in batch
            }
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    results.append(res)
                    api_calls += 1
                    processed += 1
                    pbar.update(1)

                    # Update streamlit progress
                    if progress_bar is not None:
                        progress = processed / total
                        progress_bar.progress(progress)

                    # Update caption with ETA
                    update_caption_with_eta(processed, total)

                    if api_calls % 60 == 0:
                        print(
                            "[RateLimiter] ⏸ Pausing for 10 seconds to respect API rate limits..."
                        )
                        time.sleep(10)

                    if processed % 30 == 0:
                        # checkpoint every 30 processed
                        ckpt.state["r1_pending"] = pending
                        ckpt.state["r1_results"] = results
                        ckpt.last_progress = (
                            processed / total
                        )  # Save progress for main pipeline
                        ckpt.save()
                        print(
                            f"Checkpoint saved at {processed}/{total} rows processed."
                        )
                except Exception as e:
                    error_msg = f"Round1 error: {e}"
                    print(error_msg)
                    st.error(error_msg)

    # final checkpoint
    ckpt.state["r1_pending"] = pending
    ckpt.state["r1_results"] = results
    ckpt.last_progress = processed / total  # Save final progress
    ckpt.save()

    pbar.close()
    return results
