# file: services/llm_pipeline/resume_round_2.py
import pandas as pd
import hashlib
import time

# Removed streamlit import
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from services.llm_pipeline.r2_utils import *
from services.checkpoint.checkpoint_manager import CheckpointManager
from config import skill_proficiency_level_details
from services.db.data_writers import (
    write_r2_raw_to_s3,
    write_missing_to_s3,
)
from services.db.data_loaders import (
    load_r1_valid,
    load_sector_file,
)


def resume_round_2(
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

    # 4) Process in batches of 10
    while pending:

        batch_idx = pending[:10]
        pending = pending[10:]
        batch_df = data.iloc[batch_idx].reset_index(drop=True)

        with ThreadPoolExecutor(max_workers=10) as exec:
            futures = []
            for _, row in batch_df.iterrows():
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
                    print(f"[ERROR] {error_msg}")  # Changed from st.error to print
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
                if api_calls % 60 == 0:
                    print(
                        "[RateLimiter] ⏸ Pausing for 10 seconds to respect API rate limits..."
                    )
                    time.sleep(10)

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
    write_r2_raw_to_s3(merged, target_sector_alias)
    # e) Split untagged vs tagged
    r2_untagged = merged[merged.proficiency_level_rac_chart == 0]
    r2_tagged = merged[merged.proficiency_level_rac_chart > 0].reset_index(drop=True)

    # f) Sanity-check vs SFw
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
        skill_matches = sfw_sets[sfw_sets["skill_lower"] == skill.lower().strip()]
        if not skill_matches.empty:
            valid = skill_matches["Proficiency Level"].iloc[0]
        else:
            valid = set()
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
            .infer_objects()
        )

        vf["invalid_pl"] = vf["invalid_pl"].astype(int)

        r2_valid = vf[vf.invalid_pl == 9].drop(columns=["invalid_pl", "skill"])
        bad2 = vf[vf.invalid_pl < 9].drop(columns=["invalid_pl", "skill"])
        r2_invalid = pd.concat([r2_untagged, bad2], ignore_index=True)

    # h) Merge with R1 valid, save all three files
    r1_valid = load_r1_valid()

    r2_vout = r2_valid.copy()
    r2_vout["proficiency_level"] = r2_vout["proficiency_level_rac_chart"]
    r2_vout["reason"] = r2_vout["reason_rac_chart"]
    r2_vout.drop(
        columns=[
            "proficiency_level_rac_chart",
            "reason_rac_chart",
            "confidence_rac_chart",
        ],
        inplace=True,
    )

    # Concatenate first, then handle duplicate columns
    all_valid = pd.concat([r1_valid, r2_vout], ignore_index=True)

    # Debug: Print column names before cleaning
    print(f"[DEBUG] Columns before cleaning: {list(all_valid.columns)}")
    print(
        f"[DEBUG] Duplicate columns found: {[col for col in all_valid.columns if list(all_valid.columns).count(col) > 1]}"
    )

    # Remove any duplicate columns using pandas loc with unique column selection
    # This is more robust than manual iteration
    all_valid = all_valid.loc[:, ~all_valid.columns.duplicated()]

    # Debug: Print column names after duplicate removal
    print(f"[DEBUG] Columns after duplicate removal: {list(all_valid.columns)}")

    # Check for column existence in the concatenated dataframe before dropping
    columns_to_drop = ["invalid_pl", "Skill Title_y"]
    columns_to_drop = [col for col in columns_to_drop if col in all_valid.columns]

    # Drop the identified columns
    if columns_to_drop:
        all_valid = all_valid.drop(columns=columns_to_drop, errors="ignore")

    # Handle the Skill Title columns - rename _x version and drop any duplicate
    if "Skill Title_x" in all_valid.columns:
        all_valid = all_valid.rename(columns={"Skill Title_x": "Skill Title"})
        # After renaming, remove duplicates again if any were created
        all_valid = all_valid.loc[:, ~all_valid.columns.duplicated()]

    # Debug: Print final column names
    print(f"[DEBUG] Final columns: {list(all_valid.columns)}")
    print(
        f"[DEBUG] Any remaining duplicates: {[col for col in all_valid.columns if list(all_valid.columns).count(col) > 1]}"
    )

    # i) Poor-data-quality courses
    # Calling the S3 load_sector_file directly from services.db.data_readers
    orig = load_sector_file()
    raw_course = load_sector_file(cols=["Skill Title", "Course Reference Number"])

    merged_crs = pd.merge(orig, raw_course, on="Course Reference Number", how="inner")

    poor = merged_crs[
        ~merged_crs["Course Reference Number"].isin(merged["Course Reference Number"])
    ]

    missing = poor[poor["Course Title"].isnull()]
    write_missing_to_s3(missing, target_sector_alias)

    print("[Round 2 Complete] All processing complete, results saved to files.")
    r2_invalid = pd.concat([r2_untagged, r2_invalid], ignore_index=True)
    # The return type here is a tuple of DataFrames
    return r2_valid, r2_invalid, all_valid
