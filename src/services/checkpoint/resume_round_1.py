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

    pbar = tqdm(
        total=total, initial=len(results), desc="Round1 rows processed", unit="row"
    )

    skill_info, lock = {}, Lock()
    api_calls = len(results)
    processed = len(results)

    # Track start time for better estimation
    start_time = time.time()

    def update_caption_with_eta(processed, total, api_calls):
        if caption is not None:
            remaining = total - processed
            elapsed = time.time() - start_time

            if processed > 0:
                # Calculate how many sleep periods we've had and will have
                sleeps_completed = api_calls // 60
                total_sleep_time_so_far = sleeps_completed * 10  # 10s per sleep

                # Calculate remaining sleeps
                remaining_calls = remaining
                current_batch_position = api_calls % 60

                # Calculate future sleeps more accurately
                if current_batch_position + remaining_calls <= 60:
                    # All remaining calls fit in current batch
                    future_sleeps = 0
                else:
                    # Calculate how many complete batches remain after current
                    calls_to_complete_current_batch = 60 - current_batch_position
                    calls_after_current_batch = (
                        remaining_calls - calls_to_complete_current_batch
                    )
                    future_sleeps = 1 + (calls_after_current_batch // 60)

                # Pure processing time (without sleeps)
                pure_processing_time = elapsed - total_sleep_time_so_far

                if pure_processing_time > 0:
                    # Calculate rate based on pure processing time
                    actual_processing_rate = processed / pure_processing_time

                    # Estimate remaining processing time
                    remaining_processing_time = remaining / actual_processing_rate

                    # Add future sleep time
                    remaining_sleep_time = future_sleeps * 10

                    # Total ETA
                    eta_seconds = remaining_processing_time + remaining_sleep_time
                else:
                    # Fallback if we haven't processed enough yet
                    eta_seconds = remaining * 0.6  # rough estimate: 0.6s per item
            else:
                # Initial estimate - be conservative
                estimated_sleeps = total // 60
                eta_seconds = (total * 0.6) + (
                    estimated_sleeps * 10
                )  # Format user-friendly time display
            if eta_seconds > 3600:  # More than 1 hour
                hours = int(eta_seconds // 3600)
                minutes = int((eta_seconds % 3600) // 60)
                if minutes > 0:
                    time_display = f"About {hours} hour{'s' if hours > 1 else ''} and {minutes} minute{'s' if minutes > 1 else ''} remaining"
                else:
                    time_display = (
                        f"About {hours} hour{'s' if hours > 1 else ''} remaining"
                    )
            elif eta_seconds > 60:  # More than 1 minute
                minutes = int((eta_seconds / 60) + 0.5)  # Round up
                time_display = (
                    f"About {minutes} minute{'s' if minutes > 1 else ''} remaining"
                )
            else:
                time_display = "Less than 1 minute remaining"

            caption.caption(f"[Status] Processing 1st Stage... {time_display}")

    # Initial caption update
    update_caption_with_eta(processed, total, api_calls)

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
                    update_caption_with_eta(processed, total, api_calls)

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
