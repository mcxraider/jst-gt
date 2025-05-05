import hashlib
import concurrent.futures
from openai import OpenAI
from datetime import datetime
import logging

timestamp = datetime.now().strftime("%Y%m%d_%H%M")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=f"round_1_{timestamp}.log",
    filemode="w",
)

from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")



def generate_hash(text):
    text = str(text).lower().strip()
    string_hash = hashlib.sha256(str.encode(text)).hexdigest()
    return string_hash


def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
    client = openai.OpenAI(api_key=api_key, base_url="https://ai-api.analytics.gov.sg")

    try:
        response = client.chat.completions.create(
            model=model,
            messages=sys_msg,
            response_format={"type": "json_object"},
            seed=6800,
            temperature=temperature,
        )
        completion_output = literal_eval(response.choices[0].message.content)
    except:
        completion_output = ""

    return completion_output


PROFICIENCY_LEVEL_TAGGING_SYS_PROMPT = """
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
{{
    "proficiency": "integer value of the proficiency level", 
    "reason": "text string of your reasoning", 
    "confidence": "high / medium / low"
}}
YOUR OUTPUT IS MEANT TO BE PARSED BY ANOTHER COMPUTER PROGRAM.
"""

PROFICIENCY_LEVEL_TAGGING_USER_PROMPT = """
For the training course {crs_content}, what is the most appropriate proficiency level to be tagged to the skill {skill}, based on the Knowledge Base {temp_kb_dic}, only if you need more information, refer to the Reference Document {skill_pl_reference_chart}.
Give your response in JSON format like this: {{'proficiency': <>, 'reason': <>, 'confidence': <high | medium | low>}}
"""



def form_sys_msg(kb_dic, crs_content, skill, skill_pl_reference_chart):
    temp_kb_dic = kb_dic[skill.lower().strip()]
    system_message = [
        {
            "role": "system",
            "content": PROFICIENCY_LEVEL_TAGGING_SYS_PROMPT,
        },
        {
            "role": "user",
            "content": PROFICIENCY_LEVEL_TAGGING_USER_PROMPT, #  must edit to .format this prompt
        },
    ]
    return system_message


def get_pl_tagging(row, id_list, kb_dic, skill_pl_reference_chart):
    sys_msg = form_sys_msg(
        kb_dic=kb_dic,
        crs_content=row["course_text"],
        skill=row["skill_lower"],
        skill_pl_reference_chart=skill_pl_reference_chart,
    )
    if row["unique_id"] in id_list:
        pass
    else:
        gpt_result = get_gpt_completion(sys_msg=sys_msg)

    return [row["unique_id"], gpt_result]


def get_result(
    df,
    max_worker,
    id_list,
    result_list,
    kb_dic,
    skill_pl_reference_chart,
    checkpoint_filename,
):
    def process_result(row, id_list, kb_dic, skill_pl_reference_chart):
        with open(checkpoint_filename, "a") as file:
            gpt_output = get_pl_tagging(row, id_list, kb_dic, skill_pl_reference_chart)
            print(gpt_output[0])
            id_list.append(gpt_output[0])
            result_list.append(gpt_output[1])

            file.write(str(gpt_output[0]) + "\n")
        file.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
        for _, row in df.iterrows():
            executor.submit(
                process_result, row, id_list, kb_dic, skill_pl_reference_chart
            )

        return id_list, result_list
