import hashlib
import openai
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import textwrap
<<<<<<< HEAD
from concurrent.futures import ThreadPoolExecutor, as_completed
import os 
from ast import literal_eval
from config import *
=======
import os
from ast import literal_eval
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
from tqdm import tqdm  # make sure you’ve installed tqdm (pip install tqdm)
import random

timestamp = datetime.now().strftime("%Y%m%d_%H%M")

<<<<<<< HEAD
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=f"../log_files/round_1_{timestamp}.log",
    filemode="w"
)

=======
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)

def generate_hash(text):
    text = str(text).lower().strip()
    string_hash = hashlib.sha256(str.encode(text)).hexdigest()
    return string_hash

<<<<<<< HEAD
=======

>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
# def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
#     client = openai.OpenAI(api_key=api_key,base_url="https://ai-api.analytics.gov.sg")

#     try:
#         response = client.chat.completions.create(
<<<<<<< HEAD
#             model=model, 
=======
#             model=model,
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
#             messages = sys_msg,
#             response_format={"type": "json_object"},
#             seed=6800,
#             temperature=temperature
#         )
#         completion_output = literal_eval(response.choices[0].message.content)
#     except:
#         completion_output = ""
<<<<<<< HEAD
        
=======

>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
#     return completion_output


# simulated response form chatgpt so no need API key
def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
    """Simulated LLM API call that randomly returns a predefined response."""
    sample_responses = [
        {
            "proficiency": 2,
            "reason": "The course content involves leadership and resource allocation, which aligns with Level 2: Lead small projects.",
<<<<<<< HEAD
            "confidence": "high"
=======
            "confidence": "high",
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
        },
        {
            "proficiency": 3,
            "reason": "Mentions managing resources and leading teams—activities aligned with cross-functional project management in Level 3.",
<<<<<<< HEAD
            "confidence": "medium"
=======
            "confidence": "medium",
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
        },
        {
            "proficiency": 0,
            "reason": "The course description lacks sufficient detail to confidently map to a defined level in the Knowledge Base.",
<<<<<<< HEAD
            "confidence": "low"
=======
            "confidence": "low",
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
        },
        {
            "proficiency": 1,
            "reason": "Basic scheduling and support tasks suggest an assisting role, which fits Level 1.",
<<<<<<< HEAD
            "confidence": "medium"
=======
            "confidence": "medium",
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
        },
        {
            "proficiency": 2,
            "reason": "Team leadership and task planning imply a supervisory role, typically associated with Level 2.",
<<<<<<< HEAD
            "confidence": "high"
        }
=======
            "confidence": "high",
        },
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
    ]

    return random.choice(sample_responses)


<<<<<<< HEAD
SYSTEM_PROMPT = textwrap.dedent("""
=======
SYSTEM_PROMPT = textwrap.dedent(
    """
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
    You are a helpful expert in the area of training courses and skills.
    CONTEXT:
        You need to associate the appropriate proficiency levels to skills taught through training courses.
    GIVEN INFORMATION:
        Use only these 2 sets of information for the tasks
        1. Knowledge Base that defines the knowledge and abilities associated with each skill at the respective proficiency levels.
        2. Reference Document that defines the performance expectation for skills at different proficiency levels.
    TASK:
        1. For each pair of course content and skill taught, identify the most appropriate proficiency level for the skill, using the proficiency level definitions in the Knowledge Base.
        2. Only when you need additional information, refer to the Reference Document for decision.
        3. Only tag a skill with proficiency levels that are found in the Knowledge Base corresponding to it.
        4. When you are unsure, indicate proficiency level as 0.
    OUTPUT FORMAT:
    Give your response in JSON format like this:
    {
      "proficiency": <integer>,
      "reason": "<your reasoning text>",
      "confidence": "high|medium|low"
    }
    YOUR OUTPUT IS MEANT TO BE PARSED BY ANOTHER COMPUTER PROGRAM.
<<<<<<< HEAD
""").strip()
=======
"""
).strip()

>>>>>>> b51c457 (improved r1 and r2 processing pipeline)

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
        {"role": "system", "content": SYSTEM_PROMPT},
<<<<<<< HEAD
        {"role": "user",   "content": user_prompt}
    ]

=======
        {"role": "user", "content": user_prompt},
    ]


>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
# ------------------------------------------------------------
# 3) One row → one (id, result) tuple
# ------------------------------------------------------------
def get_pl_tagging(row, kb_dic, skill_pl_reference_chart):
    sys_msg = form_sys_msg(
        kb_dic=kb_dic,
        course_text=row["course_text"],
        skill=row["skill_lower"],
<<<<<<< HEAD
        skill_pl_reference_chart=skill_pl_reference_chart
    )
    return row["unique_id"], get_gpt_completion(sys_msg=sys_msg)

=======
        skill_pl_reference_chart=skill_pl_reference_chart,
    )
    return row["unique_id"], get_gpt_completion(sys_msg=sys_msg)


>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
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

<<<<<<< HEAD
    with ThreadPoolExecutor(max_workers=max_workers) as executor, \
         open(checkpoint_filename, "a") as ckpt:
=======
    with ThreadPoolExecutor(max_workers=max_workers) as executor, open(
        checkpoint_filename, "a"
    ) as ckpt:
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)

        # 3) Submit one future per row (with tqdm)
        print("📝 Submitting tasks to ThreadPoolExecutor…")
        for _, row in tqdm(df.iterrows(), total=n, desc="Submitting", unit="row"):
<<<<<<< HEAD
            fut = executor.submit(get_pl_tagging,
                                  row,
                                  kb_dic,
                                  skill_pl_reference_chart)
=======
            fut = executor.submit(get_pl_tagging, row, kb_dic, skill_pl_reference_chart)
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
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
<<<<<<< HEAD
    return id_list, result_list
=======
    return id_list, result_list
>>>>>>> b51c457 (improved r1 and r2 processing pipeline)
