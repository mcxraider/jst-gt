# file: r2_utils.py
import hashlib
from openai import OpenAI
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm
import json  # Ensure json is imported for loads()
from models.prompt_templates import R2_SYSTEM_PROMPT
from dotenv import load_dotenv

load_dotenv()

timestamp = datetime.now().strftime("%Y%m%d_%H%M")


def generate_hash(text):
    text = str(text).lower().strip()
    string_hash = hashlib.sha256(str.encode(text)).hexdigest()
    return string_hash


def get_openai_client():
    """
    Creates and returns a new OpenAI client instance.
    Raises a ValueError if the API key is not found.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is not set.")

    # Return a new client instance for thread-safety.
    client = OpenAI(api_key=api_key, base_url="https://ai-api.analytics.gov.sg")
    return client


# The actual get_gpt_completion function (commented out for testing)
# def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
#     """
#     Calls the OpenAI API to get a completion.
#     """
#     try:
#         # Create a client instance for this specific API call/thread.
#         client = get_openai_client()

#         response = client.chat.completions.create(
#             model=model,
#             messages=sys_msg,
#             response_format={"type": "json_object"},
#             seed=6800,
#             temperature=temperature,
#         )
#         completion_output = json.loads(response.choices[0].message.content)

#     except ValueError as e:
#         # Catches the specific error from get_openai_client if API key is missing
#         print(f"[ERROR] Could not create OpenAI client: {e}")
#         completion_output = {}

#     except Exception as e:
#         # Catches other potential API or parsing errors
#         print(f"[ERROR] OpenAI API call failed in get_gpt_completion: {e}")
#         completion_output = {}

#     return completion_output


# Dummy function for testing purposes
def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
    """
    Dummy function that returns mock responses based on R2_SYSTEM_PROMPT format.
    Returns responses in the format: {'proficiency': int, 'reason': str, 'confidence': str}
    """
    import random

    # Extract user message to understand what's being asked
    user_content = ""
    for msg in sys_msg:
        if msg.get("role") == "user":
            user_content = msg.get("content", "")
            break

    # Parse skill and course from user content for more realistic responses
    skill_mentioned = "skill" in user_content.lower()
    course_mentioned = "course" in user_content.lower()

    # Dummy proficiency levels (typically 1-5 based on common frameworks)
    proficiency_levels = [1, 2, 3, 4, 5]
    confidence_levels = ["high", "medium", "low"]

    # Generate realistic dummy reasons based on common skill assessment scenarios
    reasons = [
        "Course content aligns well with level requirements and learning objectives.",
        "Training materials demonstrate practical application at this proficiency level.",
        "Learning outcomes match the knowledge and ability requirements.",
        "Course depth and complexity correspond to expected proficiency standards.",
        "Assessment criteria and activities support this proficiency classification.",
        "Content coverage spans multiple competency areas at appropriate depth.",
        "Practical exercises and case studies indicate advanced skill development.",
        "Basic concepts and foundational knowledge align with entry-level expectations.",
        "Intermediate skills development evident through course structure and content."
    ]

    # Create mock response
    mock_response = {
        "proficiency": random.choice(proficiency_levels),
        "reason": random.choice(reasons),
        "confidence": random.choice(confidence_levels)
    }

    print(f"[DUMMY] Generated mock response: {mock_response}")
    return mock_response


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
    # The get_gpt_completion function handles its own client creation, which is correct.
    return row["unique_id"], get_gpt_completion(sys_msg=sys_msg)


# ------------------------------------------------------------
# 4) Parallel execution, checkpointing, and result‐collection
# ------------------------------------------------------------
def get_result(df, max_workers, kb_dic, skill_pl_reference_chart, checkpoint_filename):
    n = len(df)
    print(f"get_result called with {n} rows")
    if n == 0:
        print("Empty DataFrame – skipping executor entirely.")
        return [], []

    os.makedirs(os.path.dirname(checkpoint_filename), exist_ok=True)

    id_list, result_list = [], []
    futures = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        print("📝 Submitting tasks to ThreadPoolExecutor…")
        for _, row in tqdm(df.iterrows(), total=n, desc="Submitting", unit="row"):
            fut = executor.submit(get_pl_tagging, row, kb_dic, skill_pl_reference_chart)
            futures[fut] = row["unique_id"]

        print("🔄 Waiting for results…")
        for fut in tqdm(as_completed(futures), total=n, desc="Processing", unit="task"):
            uid = futures[fut]
            try:
                returned_id, res = fut.result()
                id_list.append(returned_id)
                result_list.append(res)
            except Exception as e:
                print(f"❌ Failed to process ID {uid}: {e}")

    print(f"\n🏁 Finished – {len(result_list)} / {n} rows succeeded.")
    return id_list, result_list
