import time
from pathlib import Path
from datetime import datetime
import pandas as pd
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from tqdm import tqdm

from backend_utils.r1_utils import *

from backend_utils.r2_utils import *
from config import *
from backend_utils.skill_rac_chart import skill_proficiency_level_details
import streamlit as st
from services.db import *
from services.storage import *


pd.set_option("future.no_silent_downcasting", True)

NUM_ROWS = 200
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")


def wrap_valid_df_with_name(df, target_sector_alias):

    name = f"Valid Skills for {target_sector_alias} sector"
    return (df, name)


def wrap_invalid_df_with_name(df, target_sector_alias):
    name = f"Invalid Skills for {target_sector_alias}"
    return (df, name)


def wrap_all_df_with_name(df, target_sector_alias):
    name = f"All Tagged Skills for {target_sector_alias} sector"
    return (df, name)


class CheckpointManager:
    """
    Manages saving and loading of pipeline state to a pickle file.
    Abstracts I/O for easy migration to S3 or local.
    """

    def __init__(self, alias: str, TIMESTAMP: str, checkpoint_dir=None):
        if checkpoint_dir is None:
            # Default to config path (can be S3 or local)
            checkpoint_dir = checkpoint_path
        self.base_checkpoint_path = str(checkpoint_dir)
        filename = f"{alias}_checkpoint_{TIMESTAMP}.pkl"
        self.checkpoint_path = f"{self.base_checkpoint_path}/{filename}"
        self.state = {}
        self.last_progress = 0
        self.current_round = None
        self.sector = alias

    def load(self) -> bool:
        """Load the most recent checkpoint (.pkl) from local or S3."""
        # List all .pkl files
        pkl_files = list_files(self.base_checkpoint_path, "*.pkl")
        if not pkl_files:
            return False

        # Find most recently modified (for S3, you might want to sort by filename or implement S3 last-modified)
        # Here, we assume lexicographical order if S3, timestamped filename
        if isinstance(pkl_files[0], Path):
            latest_file = max(pkl_files, key=lambda p: p.stat().st_mtime)
            latest_file = str(latest_file)
        else:
            # S3: use the latest by filename (relies on TIMESTAMP in name)
            latest_file = sorted(pkl_files)[-1]
        self.checkpoint_path = latest_file

        with st.spinner("Retrieving data from previously saved checkpoint"):
            self.state = load_pickle(latest_file)

        print(f"[Checkpoint] Loaded state from {latest_file}")
        self.last_progress = self.state.get("progress", self.last_progress)
        self.current_round = self.state.get("round", self.current_round)
        self.sector = self.state.get("sector", self.sector)
        st.session_state.selected_process_alias = self.sector
        return True

    def save(self):
        """Save checkpoint (to local or S3 as a .pkl)."""
        # Calculate and store progress information
        if "r1_pending" in self.state and "r1_results" in self.state:
            total = len(self.state["r1_pending"]) + len(self.state["r1_results"])
            if total > 0:
                self.last_progress = len(self.state["r1_results"]) / total
                self.state["progress"] = self.last_progress
        elif "r2_pending" in self.state and "r2_results" in self.state:
            total = len(self.state["r2_pending"]) + len(self.state["r2_results"])
            if total > 0:
                self.last_progress = len(self.state["r2_results"]) / total
                self.state["progress"] = self.last_progress

        self.state["sector"] = st.session_state.selected_process_alias

        save_pickle(self.state, self.checkpoint_path)
        print(f"[Checkpoint] Saved state at {datetime.now()}")

        st.session_state.pkl_yes = True


def handle_core_processing(caption, target_sector, target_sector_alias):
    """
    Orchestrates Round 1 and Round 2 with checkpointing and Streamlit integration.
    """
    progress_bar = st.progress(0)

    ckpt = CheckpointManager(target_sector_alias, TIMESTAMP)

    # If checkpoint exists, try to load it
    if ckpt.load():
        caption.caption("[Status] Retrieving Checkpoint Metadata...")
        progress_bar.progress(ckpt.last_progress)

        caption.caption("[Status] Processing input files from last checkpoint...")
        try:
            return handle_checkpoint_processing(
                caption, target_sector, target_sector_alias, ckpt, progress_bar
            )
        except Exception as e:
            st.error(f"Error resuming from checkpoint: {e}")
            st.info("Restarting processing from the beginning.")

    caption.caption("[Status] Processing input files...")
    print("\n" + "-" * 80 + "\n")
    print("ROUND 1 PROCESS STARTING")
    print("\n" + "-" * 80 + "\n")

    st.toast("File processing started. Checkpoints will be saved regularly.")
    # === Round 1 Setup ===
    # load_sfw_file()
    sfw = load_sfw_file()
    sfw = sfw[sfw["Sector"].isin(target_sector)].reset_index(drop=True)
    sfw["skill_lower"] = sfw["TSC_CCS Title"].str.lower().str.strip()

    course_df = load_sector_file(
        cols=[
            "Course Reference Number",
            "Course Title",
            "Skill Title",
            "About This Course",
            "What You'll Learn",
        ],
    )
    course_df = (
        course_df.drop_duplicates(subset=["Course Reference Number", "Skill Title"])
        .dropna()
        .reset_index(drop=True)
    )
    course_df["skill_lower"] = course_df["Skill Title"].str.lower().str.strip()

    # Save immediately out-of-sector skills
    skill_set = set(sfw["skill_lower"])
    course_df["Sector Relevance"] = course_df["skill_lower"].apply(
        lambda x: "In Sector" if x in skill_set else "Not in sector"
    )
    irrelevant_initial = course_df[course_df["Sector Relevance"] == "Not in sector"]
    # irrelevant_initial.to_csv(irrelevant_output_path, index=False, encoding="utf-8")
    write_irrelevant_to_s3(irrelevant_initial, target_sector_alias)
    work_df = (
        course_df[course_df["Sector Relevance"] == "In Sector"]
        .reset_index(drop=True)
        .head(NUM_ROWS)
    )  # remove the head(90) this if need testing

    # Initialize Round 1 checkpoint state
    ckpt.state = {"round": "r1", "r1_pending": list(work_df.index), "r1_results": []}
    ckpt.save()

    # === Round 1 Execution ===
    caption.caption("[Status] Processing 1st Stage...")
    r1_results = resume_round1(work_df, sfw, ckpt, progress_bar)

    # Check if early exit was triggered
    if st.session_state.get("exit_halfway", False) and len(r1_results) < len(work_df):
        return []

    # === Round 1 Post-processing ===
    r1_df = pd.DataFrame(r1_results)
    r1_df["skill_lower"] = r1_df["Skill Title"].str.lower().str.strip()
    merged1 = work_df.merge(r1_df, on=["Course Reference Number", "skill_lower"])
    merged1["proficiency_level"] = merged1["proficiency_level"].astype(int)

    # Sanity-check
    valid1, invalid1 = [], []
    pl_map = sfw.groupby("skill_lower")["Proficiency Level"].agg(set).to_dict()
    for _, row in merged1.iterrows():
        (
            valid1
            if row["proficiency_level"] in pl_map.get(row["skill_lower"], set())
            else invalid1
        ).append(row)

    df_valid1 = pd.DataFrame(valid1)
    df_invalid1 = pd.DataFrame(invalid1)

    write_r1_valid_to_s3(df_valid1, target_sector_alias)
    write_r1_invalid_to_s3(df_invalid1, target_sector_alias)

    # === Round 2 Setup ===
    # Load course descriptions from original input (full load, then pick columns)
    print("\n" + "-" * 80 + "\n")
    print("ROUND 2 PROCESS STARTING")
    print("\n" + "-" * 80 + "\n")
    # load sector file
    all_descr = load_sector_file(
        cols=[
            "Course Reference Number",
            "Course Title",
            "Skill Title",
            "About This Course",
            "What You'll Learn",
        ],
    )
    # strip any accidental leading/trailing spaces in the headers
    all_descr.columns = all_descr.columns.str.strip()
    # now slice out exactly the four description columns
    descr_df = (
        all_descr[
            [
                "Course Reference Number",
                "Course Title",
                "About This Course",
                "What You'll Learn",
            ]
        ]
        .dropna(subset=["Course Reference Number"])
        .drop_duplicates("Course Reference Number")
    )

    # Merge invalid1 with descriptions
    df_r2_input = df_invalid1.merge(descr_df, on="Course Reference Number", how="left")

    # ——— Fix duplicate Skill Title columns ———
    if "Skill Title" not in df_r2_input.columns:
        skill_cols = [c for c in df_r2_input.columns if c.startswith("Skill Title")]
        if skill_cols:
            df_r2_input["Skill Title"] = df_r2_input[skill_cols[0]]
            df_r2_input.drop(columns=skill_cols, inplace=True)

    # ——— Coalesce Course Title, About This Course, What You'll Learn ———
    for base in ["Course Title", "About This Course", "What You'll Learn"]:
        x, y = f"{base}_x", f"{base}_y"
        if x in df_r2_input.columns and y in df_r2_input.columns:
            # prefer the _y (fresh descr_df) but fall back to _x if missing
            df_r2_input[base] = df_r2_input[y].fillna(df_r2_input[x])
            df_r2_input.drop(columns=[x, y], inplace=True)
        elif x in df_r2_input.columns:
            df_r2_input.rename(columns={x: base}, inplace=True)
        elif y in df_r2_input.columns:
            df_r2_input.rename(columns={y: base}, inplace=True)

    # Now you can safely do:
    df_r2_input["course_text"] = (
        df_r2_input["Course Title"]
        + " |: "
        + df_r2_input["About This Course"]
        + " | "
        + df_r2_input["What You'll Learn"]
    )

    # ——— Generate unique_id to match resume_round2() logic ———
    df_r2_input["unique_text"] = df_r2_input["course_text"] + df_r2_input["Skill Title"]
    df_r2_input["unique_id"] = (
        df_r2_input["unique_text"]
        .str.lower()
        .apply(lambda t: hashlib.sha256(t.encode()).hexdigest())
    )

    # Reset progress bar for Round 2
    progress_bar.progress(0)

    # Initialize Round 2 checkpoint
    ckpt.state = {
        "round": "r2",
        "r2_pending": list(df_r2_input.index),
        "r2_results": [],
    }
    ckpt.save()
    caption.caption("[Status] Processing 2nd Stage...")

    r2_valid, r2_invalid, all_valid = resume_round2(
        target_sector, target_sector_alias, df_r2_input, sfw, ckpt, progress_bar
    )

    # Check if early exit was triggered
    if st.session_state.get("exit_halfway", False) and ckpt.state["r2_pending"]:
        return []

    st.success(f"Round 2 complete, all files saved in S3.")
    ret_r2_valid = wrap_valid_df_with_name(r2_valid, target_sector_alias)
    ret_r2_invalid = wrap_invalid_df_with_name(r2_invalid, target_sector_alias)
    ret_all_valid = wrap_all_df_with_name(all_valid, target_sector_alias)

    return [ret_r2_valid, ret_r2_invalid, ret_all_valid]


def resume_round1(work_df, sfw_df, ckpt, progress_bar=None):
    """
    Batch-process Round 1 prompts with:
      - 10 workers
      - Sleep 10s every 40 calls
      - Checkpoint every 30
      - Show tqdm progress bar and Streamlit progress
    """
    # client = get_openai_client(api_key, base_url)
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

    # Early exit check
    stop_number = total // 2  # halfway point

    while pending:
        # Check for early exit toggle
        if st.session_state.get("exit_halfway", False) and processed >= stop_number:
            break

        batch = pending[:10]
        pending = pending[10:]
        rows = work_df.loc[batch]

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    process_row, rows.loc[idx], skill_info, sfw_df, lock, client
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

                    if api_calls % 40 == 0:
                        print(
                            "[RateLimiter] ⏸ Pausing for 10 seconds to respect API rate limits..."
                        )
                        time.sleep(1)

                    if processed % 30 == 0:
                        # checkpoint every 30 processed
                        ckpt.state["r1_pending"] = pending
                        ckpt.state["r1_results"] = results
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
    ckpt.save()

    pbar.close()
    return results


def resume_round2(
    target_sector: str,
    target_sector_alias: str,
    df_invalid: pd.DataFrame,
    sfw_raw: pd.DataFrame,
    ckpt: CheckpointManager,
    progress_bar=None,
):
    """
    Process Round 2 on the "invalid" from Round 1, exactly as in round2_processing.py,
    with batching (10 at a time), a 50s pause every 40 API calls, a checkpoint every 30 rows,
    and a tqdm progress bar plus Streamlit progress updates.
    """

    # 1) Reconstruct the original "data"
    data = df_invalid.copy()
    data["course_text"] = (
        data["Course Title"]
        + " |: "
        + data["About This Course"]
        + " | "
        + data["What You'll Learn"]
    )
    data["unique_text"] = data["course_text"] + data["Skill Title"]
    data["unique_id"] = data["unique_text"].apply(
        lambda x: hashlib.sha256(x.lower().encode()).hexdigest()
    )

    # 2) Build KB dictionary
    kb_dic = (
        sfw_raw.query("Sector in @target_sector")
        .assign(
            skill_lower=lambda df: df["TSC_CCS Title"].str.lower().str.strip(),
            items=lambda df: df["Knowledge / Ability Items"].fillna(""),
        )
        .groupby("skill_lower")
        .apply(
            lambda sub: (
                sub.groupby("Proficiency Level")["items"]
                .apply(", ".join)
                .reset_index()
                .to_dict(orient="records")
            ),
            include_groups=False,
        )
        .to_dict()
    )

    # 3) Prepare for batching & progress
    pending = ckpt.state.get("r2_pending", list(range(len(data))))
    results = ckpt.state.get("r2_results", [])
    api_calls = len(results)
    processed = len(results)

    total = len(pending) + len(results)
    pbar = tqdm(
        total=total, initial=len(results), desc="Round2 rows processed", unit="row"
    )

    # Early exit check
    stop_number = total // 2  # halfway point

    # 4) Process in batches of 10
    while pending:
        # Check for early exit toggle
        if st.session_state.get("exit_halfway", False) and processed >= stop_number:
            break

        batch_idx = pending[:10]
        pending = pending[10:]
        batch_df = data.iloc[batch_idx].reset_index(drop=True)

        with ThreadPoolExecutor(max_workers=10) as exec:
            futures = []
            for i, row in batch_df.iterrows():
                sys_msg = form_sys_msg(
                    kb_dic,
                    row["course_text"],
                    row["Skill Title"],
                    skill_proficiency_level_details,
                )
                futures.append(
                    (row["unique_id"], exec.submit(get_gpt_completion, sys_msg))
                )

            for uid, fut in futures:
                try:
                    out = fut.result()
                    results.append(
                        {
                            "unique_id": uid,
                            "pl": out.get("proficiency", 0),
                            "reason": out.get("reason", ""),
                            "confidence": out.get("confidence", ""),
                        }
                    )
                except Exception as e:
                    error_msg = f"Round2 failed for {uid}: {e}"
                    st.error(error_msg)
                    results.append(
                        {"unique_id": uid, "pl": 0, "reason": "", "confidence": ""}
                    )

                api_calls += 1
                processed += 1
                pbar.update(1)

                # Update streamlit progress
                if progress_bar is not None:
                    progress = processed / total
                    progress_bar.progress(progress)

                # rate‐limit pause
                if api_calls % 40 == 0:
                    print(
                        "[RateLimiter] ⏸ Pausing for 10 seconds to respect API rate limits..."
                    )
                    time.sleep(1)

                # checkpoint every 30 rows
                if processed % 30 == 0:
                    ckpt.state["r2_pending"] = pending
                    ckpt.state["r2_results"] = results
                    ckpt.save()
                    print(f"Checkpoint saved at {processed}/{total} rows processed.")

    # final checkpoint
    ckpt.state["r2_pending"] = pending
    ckpt.state["r2_results"] = results
    ckpt.save()
    print("Final Round 2 checkpoint saved.")

    pbar.close()

    result_df = pd.DataFrame(
        {
            "unique_id": [r["unique_id"] for r in results],
            "proficiency_level_rac_chart": [r["pl"] for r in results],
            "reason_rac_chart": [r["reason"] for r in results],
            "confidence_rac_chart": [r["confidence"] for r in results],
        }
    )

    # b) Merge back into data
    sub = result_df[result_df.unique_id.isin(data.unique_id)]
    merged = data.merge(sub, on="unique_id", how="left")

    # c) Diagnostics
    num_untagged = (merged.proficiency_level_rac_chart == 0).sum()
    num_processed = sub.shape[0]
    total = data.shape[0]
    print(f"[Round 2 Summary] 🏷️ Untagged skills remaining: {num_untagged}")
    print(f"[Round 2 Summary] ✅ Skills processed in this round: {num_processed}")
    print(f"[Round 2 Summary] 🔄 Skills carried over from Round 1: {total}")

    print(
        "[Round 2 Post-processing] 🗂️ Finalizing and exporting results to CSV files..."
    )

    # d) Drop helper columns and save raw
    merged.drop(columns=["course_text", "unique_text", "unique_id"], inplace=True)
    merged["proficiency_level"] = merged["proficiency_level"].astype(
        int
    )  # original R1 PL
    # merged.to_csv(r2_raw_output_path, index=False, encoding="utf-8")
    write_r2_raw_to_s3(merged, target_sector_alias)
    # e) Split untagged vs tagged
    r2_untagged = merged[merged.proficiency_level_rac_chart == 0]
    r2_tagged = merged[merged.proficiency_level_rac_chart > 0].reset_index(drop=True)

    # f) Sanity‐check vs SFw
    sanity = (
        r2_tagged.groupby("Skill Title")["proficiency_level_rac_chart"]
        .agg(set)
        .reset_index()
    )
    sfw_sets = (
        sfw_raw.groupby("TSC_CCS Title")["Proficiency Level"]
        .agg(set)
        .reset_index()
        .assign(skill_lower=lambda df: df["TSC_CCS Title"].str.lower().str.strip())
    )

    violations = []
    for skill, plset in zip(
        sanity["Skill Title"], sanity["proficiency_level_rac_chart"]
    ):
        matches = sfw_sets.loc[
            sfw_sets["skill_lower"] == skill.lower().strip(), "Proficiency Level"
        ]
        valid = matches.iloc[0] if not matches.empty else set()
        bad = [p for p in plset if p not in valid]
        if bad:
            violations.append({"skill": skill, "invalid_pl": bad})

    # g) Build final valid/invalid sets
    if not violations:
        # all tagged are fine
        r2_valid = r2_tagged
        r2_invalid = r2_untagged.copy()
    else:
        vdf = (
            pd.DataFrame(violations)
            .explode("invalid_pl")
            .assign(skill_lower=lambda df: df.skill.str.lower().str.strip())
        )
        vf = (
            pd.merge(
                r2_tagged,
                vdf,
                left_on=["skill_lower", "proficiency_level_rac_chart"],
                right_on=["skill_lower", "invalid_pl"],
                how="outer",
            )
            .fillna(9)
            .infer_objects(copy=False)
        )

        vf["invalid_pl"] = vf["invalid_pl"].astype(int)

        r2_valid = vf[vf.invalid_pl == 9].drop(columns=["invalid_pl", "skill"])
        bad2 = vf[vf.invalid_pl < 9].drop(columns=["invalid_pl", "skill"])
        r2_invalid = pd.concat([r2_untagged, bad2], ignore_index=True)

    # h) Merge with R1 valid, save all three files
    r1_valid = load_r1_valid()

    # r1_valid = pd.read_csv(
    #     round_1_valid_output_path, low_memory=False, encoding="utf-8"
    # )
    r2_vout = r2_valid.copy()
    r2_vout["proficiency_level"] = r2_vout["proficiency_level_rac_chart"]
    r2_vout["reason"] = r2_vout["reason_rac_chart"]
    r2_vout["confidence"] = r2_vout["confidence_rac_chart"]
    r2_vout.drop(
        columns=[
            "proficiency_level_rac_chart",
            "reason_rac_chart",
            "confidence_rac_chart",
        ],
        inplace=True,
    )

    all_valid = pd.concat([r1_valid, r2_vout], ignore_index=True).drop(
        columns=["invalid_pl"], errors="ignore"
    )

    # i) Poor-data-quality courses
    orig = load_sector_file()

    # raw_course is now an .xlsx, so use read_excel
    raw_course = load_sector_file(cols=["Skill Title", "Course Reference Number"])

    # merge on the shared key
    merged_crs = pd.merge(orig, raw_course, on="Course Reference Number", how="inner")

    # identify "poor" (i.e. not already merged)
    poor = merged_crs[
        ~merged_crs["Course Reference Number"].isin(merged["Course Reference Number"])
    ]

    # split out those with completely missing titles
    missing = poor[poor["Course Title"].isnull()]

    # and the rest
    rest = poor[
        ~poor["Course Reference Number"].isin(missing["Course Reference Number"])
    ]

    # write out as UTF-8 CSVs
    missing.to_csv(
        f"{misc_output_path}/{target_sector_alias}_missing_content_course_{TIMESTAMP}.csv",
        index=False,
        encoding="utf-8",
    )
    rest.to_csv(
        f"{misc_output_path}/{target_sector_alias}_poor_content_quality_course_{TIMESTAMP}.csv",
        index=False,
        encoding="utf-8",
    )

    # these are the completed outputs
    print("[Round 2 Complete] All processing complete, results saved to files.")
    r2_invalid = pd.concat([r2_untagged, r2_invalid], ignore_index=True)
    return r2_valid, r2_invalid, all_valid


def handle_checkpoint_processing(
    caption, target_sector, target_sector_alias, ckpt, progress_bar=None
):
    """
    Resumes processing from the checkpoint based on which round was active.
    """
    state = ckpt.state
    if state.get("round") == "r1":

        # Load necessary files
        print("\n" + "-" * 80 + "\n")
        print("ROUND 1 PROCESS STARTING")
        print("\n" + "-" * 80 + "\n")
        sfw = load_sfw_file()
        sfw = sfw[sfw["Sector"].isin(target_sector)].reset_index(drop=True)
        sfw["skill_lower"] = sfw["TSC_CCS Title"].str.lower().str.strip()

        course_df = load_sector_file(
            cols=[
                "Course Reference Number",
                "Course Title",
                "Skill Title",
                "About This Course",
                "What You'll Learn",
            ],
        )
        course_df = (
            course_df.drop_duplicates(subset=["Course Reference Number", "Skill Title"])
            .dropna()
            .reset_index(drop=True)
        )

        # Add these lines to match the preprocessing in handle_core_processing
        course_df["skill_lower"] = course_df["Skill Title"].str.lower().str.strip()
        skill_set = set(sfw["skill_lower"])
        course_df["Sector Relevance"] = course_df["skill_lower"].apply(
            lambda x: "In Sector" if x in skill_set else "Not in sector"
        )

        # Save immediately out-of-sector skills if needed
        irrelevant_initial = course_df[course_df["Sector Relevance"] == "Not in sector"]
        # Save irrelevant skills
        # irrelevant_initial.to_csv(irrelevant_output_path, index=False, encoding="utf-8")
        write_irrelevant_to_s3(irrelevant_initial, target_sector_alias)
        work_df = (
            course_df[course_df["Sector Relevance"] == "In Sector"]
            .reset_index(drop=True)
            .head(NUM_ROWS)  # Keep the same limit as in original function
        )

        # Resume Round 1
        caption.caption("[Status] Processing 1st Stage...")
        r1_results = resume_round1(work_df, sfw, ckpt, progress_bar)

        # Check if early exit was triggered
        if st.session_state.get("exit_halfway", False) and len(r1_results) < len(
            work_df
        ):
            return []

        # === Round 1 Post-processing ===
        r1_df = pd.DataFrame(r1_results)
        r1_df["skill_lower"] = r1_df["Skill Title"].str.lower().str.strip()
        merged1 = work_df.merge(r1_df, on=["Course Reference Number", "skill_lower"])
        merged1["proficiency_level"] = merged1["proficiency_level"].astype(int)

        # Sanity-check
        valid1, invalid1 = [], []
        pl_map = sfw.groupby("skill_lower")["Proficiency Level"].agg(set).to_dict()
        for _, row in merged1.iterrows():
            (
                valid1
                if row["proficiency_level"] in pl_map.get(row["skill_lower"], set())
                else invalid1
            ).append(row)

        df_valid1 = pd.DataFrame(valid1)
        df_invalid1 = pd.DataFrame(invalid1)
        # df_valid1.to_csv(round_1_valid_output_path, index=False, encoding="utf-8")
        # df_invalid1.to_csv(round_1_invalid_output_path, index=False, encoding="utf-8")
        write_r1_valid_to_s3(df_valid1, target_sector_alias)
        write_r1_invalid_to_s3(df_invalid1, target_sector_alias)
        # === Round 2 Setup ===
        print("\n" + "-" * 80 + "\n")
        print("ROUND 2 PROCESS STARTING")
        print("\n" + "-" * 80 + "\n")
        # Load course descriptions from original input (full load, then pick columns)
        all_descr = load_sector_file(
            cols=[
                "Course Reference Number",
                "Course Title",
                "Skill Title",
                "About This Course",
                "What You'll Learn",
            ],
        )
        # strip any accidental leading/trailing spaces in the headers
        all_descr.columns = all_descr.columns.str.strip()
        # now slice out exactly the four description columns
        descr_df = (
            all_descr[
                [
                    "Course Reference Number",
                    "Course Title",
                    "About This Course",
                    "What You'll Learn",
                ]
            ]
            .dropna(subset=["Course Reference Number"])
            .drop_duplicates("Course Reference Number")
        )

        # Merge invalid1 with descriptions
        df_r2_input = df_invalid1.merge(
            descr_df, on="Course Reference Number", how="left"
        )

        # ——— Fix duplicate Skill Title columns ———
        if "Skill Title" not in df_r2_input.columns:
            skill_cols = [c for c in df_r2_input.columns if c.startswith("Skill Title")]
            if skill_cols:
                df_r2_input["Skill Title"] = df_r2_input[skill_cols[0]]
                df_r2_input.drop(columns=skill_cols, inplace=True)

        # ——— Coalesce Course Title, About This Course, What You'll Learn ———
        for base in ["Course Title", "About This Course", "What You'll Learn"]:
            x, y = f"{base}_x", f"{base}_y"
            if x in df_r2_input.columns and y in df_r2_input.columns:
                # prefer the _y (fresh descr_df) but fall back to _x if missing
                df_r2_input[base] = df_r2_input[y].fillna(df_r2_input[x])
                df_r2_input.drop(columns=[x, y], inplace=True)
            elif x in df_r2_input.columns:
                df_r2_input.rename(columns={x: base}, inplace=True)
            elif y in df_r2_input.columns:
                df_r2_input.rename(columns={y: base}, inplace=True)

        # Now you can safely do:
        df_r2_input["course_text"] = (
            df_r2_input["Course Title"]
            + " |: "
            + df_r2_input["About This Course"]
            + " | "
            + df_r2_input["What You'll Learn"]
        )

        # ——— Generate unique_id to match resume_round2() logic ———
        df_r2_input["unique_text"] = (
            df_r2_input["course_text"] + df_r2_input["Skill Title"]
        )
        df_r2_input["unique_id"] = (
            df_r2_input["unique_text"]
            .str.lower()
            .apply(lambda t: hashlib.sha256(t.encode()).hexdigest())
        )

        # Reset progress bar for Round 2
        if progress_bar:
            progress_bar.progress(0)

        # Initialize Round 2 checkpoint
        ckpt.state = {
            "round": "r2",
            "r2_pending": list(df_r2_input.index),
            "r2_results": [],
        }
        ckpt.save()
        caption.caption("[Status] Processing 2nd Stage...")

        # Start Round 2 processing
        r2_valid, r2_invalid, all_valid = resume_round2(
            target_sector, target_sector_alias, df_r2_input, sfw, ckpt, progress_bar
        )

        # Check if early exit was triggered
        if st.session_state.get("exit_halfway", False) and ckpt.state["r2_pending"]:
            return []

        st.success(f"Round 2 complete, all files saved in S3.")
        ret_r2_valid = wrap_valid_df_with_name(r2_valid, target_sector_alias)
        ret_r2_invalid = wrap_invalid_df_with_name(r2_invalid, target_sector_alias)
        ret_all_valid = wrap_all_df_with_name(all_valid, target_sector_alias)

        return [ret_r2_valid, ret_r2_invalid, ret_all_valid]

    elif state.get("round") == "r2":

        # Load necessary files (similar to Round 1 but for Round 2)
        sfw = load_sfw_file()
        sfw = sfw[sfw["Sector"].isin(target_sector)].reset_index(drop=True)

        # Load the df_r2_input from r2 checkpoint state
        # We need to recreate the input DataFrame from Round 1 invalid results
        # First, load the invalid results from Round 1
        df_invalid1 = load_r1_invalid()

        # df_invalid1 = pd.read_csv(round_1_invalid_output_path)

        # Load course descriptions
        all_descr = load_sector_file(
            cols=[
                "Course Reference Number",
                "Course Title",
                "Skill Title",
                "About This Course",
                "What You'll Learn",
            ],
        )
        all_descr.columns = all_descr.columns.str.strip()
        descr_df = (
            all_descr[
                [
                    "Course Reference Number",
                    "Course Title",
                    "About This Course",
                    "What You'll Learn",
                ]
            ]
            .dropna(subset=["Course Reference Number"])
            .drop_duplicates("Course Reference Number")
        )

        # Merge invalid1 with descriptions
        df_r2_input = df_invalid1.merge(
            descr_df, on="Course Reference Number", how="left"
        )

        # Fix duplicate columns and process as in Round 1->2 transition
        # ——— Fix duplicate Skill Title columns ———
        if "Skill Title" not in df_r2_input.columns:
            skill_cols = [c for c in df_r2_input.columns if c.startswith("Skill Title")]
            if skill_cols:
                df_r2_input["Skill Title"] = df_r2_input[skill_cols[0]]
                df_r2_input.drop(columns=skill_cols, inplace=True)

        # ——— Coalesce Course Title, About This Course, What You'll Learn ———
        for base in ["Course Title", "About This Course", "What You'll Learn"]:
            x, y = f"{base}_x", f"{base}_y"
            if x in df_r2_input.columns and y in df_r2_input.columns:
                df_r2_input[base] = df_r2_input[y].fillna(df_r2_input[x])
                df_r2_input.drop(columns=[x, y], inplace=True)
            elif x in df_r2_input.columns:
                df_r2_input.rename(columns={x: base}, inplace=True)
            elif y in df_r2_input.columns:
                df_r2_input.rename(columns={y: base}, inplace=True)

        # Recreate course_text and unique_id
        df_r2_input["course_text"] = (
            df_r2_input["Course Title"]
            + " |: "
            + df_r2_input["About This Course"]
            + " | "
            + df_r2_input["What You'll Learn"]
        )

        df_r2_input["unique_text"] = (
            df_r2_input["course_text"] + df_r2_input["Skill Title"]
        )
        df_r2_input["unique_id"] = (
            df_r2_input["unique_text"]
            .str.lower()
            .apply(lambda t: hashlib.sha256(t.encode()).hexdigest())
        )

        # Resume Round 2 processing
        r2_valid, r2_invalid, all_valid = resume_round2(
            target_sector, target_sector_alias, df_r2_input, sfw, ckpt, progress_bar
        )

        st.success(f"Round 2 complete after resuming from checkpoint, all files saved.")
        ret_r2_valid = wrap_valid_df_with_name(r2_valid, target_sector_alias)
        ret_r2_invalid = wrap_invalid_df_with_name(r2_invalid, target_sector_alias)
        ret_all_valid = wrap_all_df_with_name(all_valid, target_sector_alias)

        return [ret_r2_valid, ret_r2_invalid, ret_all_valid]
