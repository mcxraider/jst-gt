# file: resume_round_1.py
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Note: The function is imported as 'process_row', so the typo in your code snippet should be corrected.
from services.llm_pipeline.r1_utils import process_row


def resume_round_1(work_df, sfw_df, progress_bar=None):
    """
    Batch-process Round 1 prompts without checkpointing.
    """
    # The client is created inside process_row now, so we don't need it here.
    # client = get_openai_client(api_key, base_url)
    # The line `client = None` is no longer needed.

    results = []
    total = len(work_df)
    pbar = tqdm(total=total, desc="Round1 rows processed", unit="row")

    skill_info, lock = {}, Lock()
    api_calls = 0
    processed = 0

    # Process all rows at once, as checkpointing is disabled
    with ThreadPoolExecutor(max_workers=18) as executor:
        futures = {
            executor.submit(process_row, row, skill_info, sfw_df, lock): idx
            for idx, row in work_df.iterrows()
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

            except Exception as e:
                error_msg = f"Round1 error: {e}"
                print(error_msg)

    pbar.close()
    return results
