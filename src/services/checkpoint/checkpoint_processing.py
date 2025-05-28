import pandas as pd
import hashlib
import streamlit as st

from services.db import *
from services.checkpoint.resume_round_1 import resume_round_1
from services.checkpoint.resume_round_2 import resume_round_2
from utils.processing_utils import *


NUM_ROWS = 200


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
        write_irrelevant_to_s3(irrelevant_initial, target_sector_alias)
        work_df = (
            course_df[course_df["Sector Relevance"] == "In Sector"]
            .reset_index(drop=True)
            .head(NUM_ROWS)  # Keep the same limit as in original function
        )

        # Resume Round 1
        caption.caption("[Status] Processing 1st Stage...")
        r1_results = resume_round_1(work_df, sfw, ckpt, progress_bar)

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
        r2_valid, r2_invalid, all_valid = resume_round_2(
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
        r2_valid, r2_invalid, all_valid = resume_round_2(
            target_sector, target_sector_alias, df_r2_input, sfw, ckpt, progress_bar
        )

        st.success(f"Round 2 complete after resuming from checkpoint, all files saved.")
        ret_r2_valid = wrap_valid_df_with_name(r2_valid, target_sector_alias)
        ret_r2_invalid = wrap_invalid_df_with_name(r2_invalid, target_sector_alias)
        ret_all_valid = wrap_all_df_with_name(all_valid, target_sector_alias)

        return [ret_r2_valid, ret_r2_invalid, ret_all_valid]
