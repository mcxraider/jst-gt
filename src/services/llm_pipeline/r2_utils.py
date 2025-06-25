import hashlib
from openai import OpenAI

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm
import random
from models.prompt_templates import R2_SYSTEM_PROMPT

# Add these imports for environment variables
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

timestamp = datetime.now().strftime("%Y%m%d_%H%M")


def generate_hash(text):
    text = str(text).lower().strip()
    string_hash = hashlib.sha256(str.encode(text)).hexdigest()
    return string_hash


def get_openai_client():
    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"), base_url="https://ai-api.analytics.gov.sg"
    )
    return client


# def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
#     client = get_openai_client()

#     try:
#         response = client.chat.completions.create(
#             model=model,
#             messages = sys_msg,
#             response_format={"type": "json_object"},
#             seed=6800,
#             temperature=temperature
#         )
#         completion_output = literal_eval(response.choices[0].message.content)
#     except:
#         completion_output = ""
#     return completion_output


# simulated response form chatgpt so no need API key
def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
    """Simulated LLM API call that randomly returns a predefined response."""
    sample_responses = [
        {
            "proficiency": 2,
            "reason": "The course content involves leadership and resource allocation, which aligns with Level 2: Lead small projects.",
            "confidence": "high",
        },
        {
            "proficiency": 3,
            "reason": "Mentions managing resources and leading teams—activities aligned with cross-functional project management in Level 3.",
            "confidence": "medium",
        },
        {
            "proficiency": 0,
            "reason": "The course description lacks sufficient detail to confidently map to a defined level in the Knowledge Base.",
            "confidence": "low",
        },
        {
            "proficiency": 1,
            "reason": "Basic scheduling and support tasks suggest an assisting role, which fits Level 1.",
            "confidence": "medium",
        },
        {
            "proficiency": 2,
            "reason": "Team leadership and task planning imply a supervisory role, typically associated with Level 2.",
            "confidence": "high",
        },
    ]

    return random.choice(sample_responses)


# ------------------------------------------------------------
# 2) Build the two‐message chat payload
# ------------------------------------------------------------
def form_sys_msg(kb_dic, course_text, skill, skill_pl_reference_chart):
    kb = kb_dic[skill.lower().strip()]
    user_prompt = (
        f"For the training course “{course_text}”, "
        f"what is the most appropriate proficiency level to be tagged to the skill “{skill}”, "
        f"based on the Knowledge Base {kb}? "
        f"Only if you need more info, refer to the Reference Document {skill_pl_reference_chart}. "
        "Reply in JSON as {'proficiency':<>, 'reason':<>, 'confidence':<high|medium|low>}."
    )
    return [
        {"role": "system", "content": R2_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]


# ------------------------------------------------------------
# 3) One row → one (id, result) tuple
# ------------------------------------------------------------
def get_pl_tagging(row, kb_dic, skill_pl_reference_chart):
    sys_msg = form_sys_msg(
        kb_dic=kb_dic,
        course_text=row["course_text"],
        skill=row["skill_lower"],
        skill_pl_reference_chart=skill_pl_reference_chart,
    )
    return row["unique_id"], get_gpt_completion(sys_msg=sys_msg)


# ------------------------------------------------------------
# 4) Parallel execution, checkpointing, and result‐collection
# ------------------------------------------------------------
def get_result(df, max_workers, kb_dic, skill_pl_reference_chart, checkpoint_filename):
    # 1) Early exit on emptiness
    n = len(df)
    print(f"get_result called with {n} rows")
    if n == 0:
        print("Empty DataFrame – skipping executor entirely.")
        return [], []

    # 2) Ensure checkpoint folder exists
    os.makedirs(os.path.dirname(checkpoint_filename), exist_ok=True)

    id_list, result_list = [], []
    futures = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor, open(
        checkpoint_filename, "a"
    ) as ckpt:

        # 3) Submit one future per row (with tqdm)
        print("📝 Submitting tasks to ThreadPoolExecutor…")
        for _, row in tqdm(df.iterrows(), total=n, desc="Submitting", unit="row"):
            fut = executor.submit(get_pl_tagging, row, kb_dic, skill_pl_reference_chart)
            futures[fut] = row["unique_id"]

        # 4) Collect results or log failures (with tqdm)
        print("🔄 Waiting for results…")
        for fut in tqdm(as_completed(futures), total=n, desc="Processing", unit="task"):
            uid = futures[fut]
            try:
                returned_id, res = fut.result()
                id_list.append(returned_id)
                result_list.append(res)
                ckpt.write(f"{returned_id}\n")
            except Exception as e:
                print(f"❌ Failed to process ID {uid}: {e}")

    # 5) Summary
    print(f"\n🏁 Finished – {len(result_list)} / {n} rows succeeded.")
    return id_list, result_list
