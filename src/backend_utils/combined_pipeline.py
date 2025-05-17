import time
import pickle
from pathlib import Path
import datetime
import pandas as pd
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from tqdm import tqdm

# r1 utilities (unchanged)
from backend_utils.r1_utils import *

# r2 utilities (unchanged)
from backend_utils.r2_utils import *
from backend_utils.config import *
from backend_utils.skill_rac_chart import skill_proficiency_level_details
import streamlit as st
from utils.db import *

pd.set_option("future.no_silent_downcasting", True)

num_rows = 60

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

round_1_invalid_output_path = f"{intermediate_output_path}/{target_sector_alias}_r1_invalid_skill_pl_{timestamp}.csv"  # Check for the timestamp on file version used
round_1_valid_output_path = f"{intermediate_output_path}/{target_sector_alias}_r1_valid_skill_pl_{timestamp}.csv"  # Check for the timestamp on file version used
r2_raw_output_path = (
    f"{misc_output_path}/{target_sector_alias}_course_skill_pl_rac_raw.csv"
)
r2_valid_output_path = (
    f"{output_path}/{target_sector_alias}_r2_valid_skill_pl_{timestamp}.csv"
)
r2_invalid_output_path = (
    f"{output_path}/{target_sector_alias}_r2_invalid_skill_pl_{timestamp}.csv"
)
all_valid_output_path = (
    f"{output_path}/{target_sector_alias}_all_valid_skill_pl_{timestamp}.csv"
)
course_descr_data_path = course_raw_data_path
irrelevant_output_path = (
    f"{intermediate_output_path}/{target_sector_alias}_r1_irrelevant_{timestamp}.csv"
)


class CheckpointManager:
    """
    Manages saving and loading of pipeline state to a pickle file.
    """

    def __init__(self, alias: str, timestamp: str):
        BASE_CHECKPOINT_DIR = Path("../s3_bucket/s3_checkpoint")
        filename = f"{alias}_checkpoint_{timestamp}.pkl"  # ✅ fixed here
        self.checkpoint_path = BASE_CHECKPOINT_DIR / filename
        self.state = {}

    def load(self) -> bool:
        if self.checkpoint_path.exists():
            with open(self.checkpoint_path, "rb") as f:
                self.state = pickle.load(f)
            print(f"[Checkpoint] Loaded state from {self.checkpoint_path}")
            return True
        return False

    def save(self):
        with open(self.checkpoint_path, "wb") as f:
            pickle.dump(self.state, f)
        print(f"[Checkpoint] ✅ State saved at {datetime.datetime.now()}")


def load_sfw_file():
    print("Loading in sfw file for processing...\n")
    return


def load_sector_file():
    print("Loading sector file for processing...\n")
    return


def handle_core_processing():
    """
    Orchestrates Round 1 and Round 2 with checkpointing.
    """
    ckpt = CheckpointManager(target_sector_alias, timestamp)
    if ckpt.load():
        print("Resuming from checkpoint...")
        return handle_load_from_checkpoint(ckpt)

    # === Round 1 Setup ===
    load_sfw_file()
    sfw = pd.read_excel(sfw_raw_data_path, sheet_name=sfw_raw_data_sheet)
    sfw = sfw[sfw["Sector"].isin(target_sector)].reset_index(drop=True)
    sfw["skill_lower"] = sfw["TSC_CCS Title"].str.lower().str.strip()

    load_sector_file()
    course_df = pd.read_excel(
        course_raw_data_path,
        sheet_name=target_sector_alias,
        usecols=course_data_columns,
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
    irrelevant_initial.to_csv(irrelevant_output_path, index=False, encoding="utf-8")

    work_df = (
        course_df[course_df["Sector Relevance"] == "In Sector"]
        .reset_index(drop=True)
        .head(60)
    )  # remove the head(90) this if need testing

    # Initialize Round 1 checkpoint state
    ckpt.state = {"round": "r1", "r1_pending": list(work_df.index), "r1_results": []}
    ckpt.save()

    # === Round 1 Execution ===
    r1_results = resume_round1(work_df, sfw, ckpt)

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
    df_valid1.to_csv(round_1_valid_output_path, index=False, encoding="utf-8")
    df_invalid1.to_csv(round_1_invalid_output_path, index=False, encoding="utf-8")
    print(
        f"\n\nRound 1 complete: {len(df_valid1)} valid, {len(df_invalid1)} invalid.\n\n"
    )

    # === Round 2 Setup ===
    # Load course descriptions from original input (full load, then pick columns)
    all_descr = pd.read_excel(course_raw_data_path, sheet_name=target_sector_alias)
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
    import hashlib

    df_r2_input["unique_text"] = df_r2_input["course_text"] + df_r2_input["Skill Title"]
    df_r2_input["unique_id"] = (
        df_r2_input["unique_text"]
        .str.lower()
        .apply(lambda t: hashlib.sha256(t.encode()).hexdigest())
    )
    # Initialize Round 2 checkpoint
    ckpt.state = {
        "round": "r2",
        "r2_pending": list(df_r2_input.index),
        "r2_results": [],
    }
    ckpt.save()

    all_valid = resume_round2(df_r2_input, sfw, ckpt)
    print(f"\n\nRound 2 complete, and all files saved in S3.\n\n")
    return all_valid


def resume_round1(work_df, sfw_df, ckpt):
    """
    Batch-process Round 1 prompts with:
      - 10 workers
      - Sleep 10s every 40 calls
      - Checkpoint every 30
      - Show tqdm progress bar
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

    while pending:
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

                    if api_calls % 40 == 0:
                        print(
                            "[RateLimiter] ⏸ Pausing for 10 seconds to respect API rate limits..."
                        )
                        time.sleep(10)

                    if processed % 30 == 0:
                        # checkpoint every 30 processed
                        ckpt.state["r1_pending"] = pending
                        ckpt.state["r1_results"] = results
                        ckpt.save()
                except Exception as e:
                    print(f"Round1 error: {e}")

    # final checkpoint
    ckpt.state["r1_pending"] = pending
    ckpt.state["r1_results"] = results
    ckpt.save()

    pbar.close()
    return results


def resume_round2(
    df_invalid: pd.DataFrame, sfw_raw: pd.DataFrame, ckpt: CheckpointManager
):
    """
    Process Round 2 on the “invalid” from Round 1, exactly as in round2_processing.py,
    with batching (10 at a time), a 50s pause every 40 API calls, a checkpoint every 30 rows,
    and a tqdm progress bar.
    """
    # 1) Reconstruct the original “data”
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
    pending = list(range(len(data)))
    results = []
    api_calls = 0
    processed = 0

    total = len(pending)
    pbar = tqdm(total=total, desc="Round2 rows processed", unit="row")

    # 4) Process in batches of 10
    while pending:
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
                    results.append(
                        {"unique_id": uid, "pl": 0, "reason": "", "confidence": ""}
                    )

                api_calls += 1
                processed += 1
                pbar.update(1)

                # rate‐limit pause
                if api_calls % 40 == 0:
                    print(
                        "[RateLimiter] ⏸ Pausing for 10 seconds to respect API rate limits..."
                    )
                    time.sleep(10)

                # checkpoint every 30 rows
                if processed % 30 == 0:
                    ckpt.state["r2_pending"] = pending
                    ckpt.state["r2_results"] = results
                    ckpt.save()

    # final checkpoint
    ckpt.state["r2_pending"] = pending
    ckpt.state["r2_results"] = results
    ckpt.save()

    pbar.close()

    # 5) Reassemble exactly as in your script:

    # a) Build result DataFrame
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
    merged.to_csv(r2_raw_output_path, index=False, encoding="utf-8")

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
    r1_valid = pd.read_csv(
        round_1_valid_output_path, low_memory=False, encoding="utf-8"
    )
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

    r2_invalid.to_csv(r2_invalid_output_path, index=False, encoding="utf-8")
    r2_valid.to_csv(r2_valid_output_path, index=False, encoding="utf-8")
    all_valid.to_csv(all_valid_output_path, index=False, encoding="utf-8")

    # i) Poor-data-quality courses
    orig = pd.read_excel(course_descr_data_path, engine="openpyxl")

    # raw_course is now an .xlsx, so use read_excel
    raw_course = pd.read_excel(
        course_raw_data_path,
        engine="openpyxl",
        usecols=["Skill Title", "Course Reference Number"],
    )

    # merge on the shared key
    merged_crs = pd.merge(orig, raw_course, on="Course Reference Number", how="inner")

    # identify “poor” (i.e. not already merged)
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
        f"{misc_output_path}/{target_sector_alias}_missing_content_course_{timestamp}.csv",
        index=False,
        encoding="utf-8",
    )
    rest.to_csv(
        f"{misc_output_path}/{target_sector_alias}_poor_content_quality_course_{timestamp}.csv",
        index=False,
        encoding="utf-8",
    )
    # these are the completed outputs.
    return r2_valid, pd.concat([r2_untagged, r2_invalid], ignore_index=True), all_valid


def handle_load_from_checkpoint(ckpt: CheckpointManager):
    """
    Resume processing based on saved checkpoint state.

    This will pick up either mid-Round 1 or mid-Round 2 and complete
    the remaining work, ultimately returning the combined valid results.
    """
    state = ckpt.state
    # Reload shared resources
    # SFW framework
    sfw = pd.read_excel(sfw_raw_data_path, sheet_name=sfw_raw_data_sheet)
    sfw = sfw[sfw["Sector"].isin(target_sector)].reset_index(drop=True)
    sfw["skill_lower"] = sfw["TSC_CCS Title"].str.lower().str.strip()

    if state.get("round") == "r1":
        # --- Reconstruct Round 1 work_df ---
        course_df = pd.read_excel(
            course_raw_data_path,
            sheet_name=target_sector_alias,
            usecols=course_data_columns,
        )
        course_df = (
            course_df.drop_duplicates(subset=["Course Reference Number", "Skill Title"])
            .dropna()
            .reset_index(drop=True)
        )
        course_df["skill_lower"] = course_df["Skill Title"].str.lower().str.strip()
        # Filter in-sector
        sector_set = set(sfw["skill_lower"])
        work_df = course_df[course_df["skill_lower"].isin(sector_set)].reset_index(
            drop=True
        )
        # Resume Round 1
        r1_results = resume_round1(work_df, sfw, ckpt)
        # After resume, proceed to postprocessing and Round 2 exactly as handle_processing
        # Merge, sanity-check, save R1 valid/invalid
        r1_df = pd.DataFrame(r1_results)
        r1_df["skill_lower"] = r1_df["Skill Title"].str.lower().str.strip()
        merged = work_df.merge(r1_df, on=["Course Reference Number", "skill_lower"])
        merged["proficiency_level"] = merged["proficiency_level"].astype(int)
        pl_map = sfw.groupby("skill_lower")["Proficiency Level"].agg(set).to_dict()
        valid1, invalid1 = [], []
        for _, row in merged.iterrows():
            (
                valid1
                if row["proficiency_level"] in pl_map.get(row["skill_lower"], set())
                else invalid1
            ).append(row)
        df_valid1 = pd.DataFrame(valid1)
        df_invalid1 = pd.DataFrame(invalid1)
        df_valid1.to_csv(round_1_valid_output_path, index=False)
        df_invalid1.to_csv(round_1_invalid_output_path, index=False)
        # fall through to Round 2
        state["round"] = "r2"
        state["r2_pending"] = []
        state["r2_results"] = []
        ckpt.state = state
        ckpt.save()
        # rebuild df_r2_input and continue below
    else:
        # Already in Round 2: load previous df_invalid from checkpoint or CSV
        df_invalid1 = pd.read_csv(round_1_invalid_output_path, low_memory=False)
        df_valid1 = pd.read_csv(round_1_valid_output_path, low_memory=False)

    # --- Round 2 preparation for both cases ---
    # Load full descriptions
    all_descr = pd.read_excel(course_raw_data_path, sheet_name=target_sector_alias)
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
    # Merge descriptions back onto invalid1
    df_r2_input = df_invalid1.merge(descr_df, on="Course Reference Number", how="left")
    # Coalesce any _x/_y duplicates
    for col in [
        "Course Title",
        "About This Course",
        "What You'll Learn",
        "Skill Title",
    ]:
        variants = [c for c in df_r2_input.columns if c.startswith(col)]
        if len(variants) > 1:
            df_r2_input[col] = df_r2_input[variants[1]].fillna(df_r2_input[variants[0]])
            df_r2_input.drop(columns=variants, inplace=True)
    # Build course_text and unique_id
    df_r2_input["course_text"] = (
        df_r2_input["Course Title"]
        + " |: "
        + df_r2_input["About This Course"]
        + " | "
        + df_r2_input["What You'll Learn"]
    )
    import hashlib

    df_r2_input["unique_id"] = (
        (df_r2_input["course_text"] + df_r2_input["Skill Title"])
        .str.lower()
        .apply(lambda t: hashlib.sha256(t.encode()).hexdigest())
    )

    # Resume Round 2 and return final all_valid
    r2_valid, r2_invalid, all_valid = resume_round2(df_r2_input, sfw, ckpt)
    return r2_valid, r2_invalid, all_valid


if __name__ == "__main__":
    handle_core_processing()
