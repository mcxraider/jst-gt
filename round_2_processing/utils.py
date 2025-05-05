import hashlib
import threading
import concurrent.futures


def generate_hash(text):
    text = str(text).lower().strip()
    string_hash = hashlib.sha256(str.encode(text)).hexdigest()
    return string_hash


# def form_sys_msg(pe_kb_dic, crs_content, skill):
#     temp_kb_dic = pe_kb_dic[skill]
#     system_message = [
#         {
#             "role": "system",
#             "content": """You are a helpful expert in the area of training courses and skills, you are helping me with associating the correct skills proficiency levels to training course contents, based on each skill's performance expectation at various proficiency levels.
#             You will be given a training course content and the corresponding skill, together with the skill's performance expectations at different levels of proficiency. Help me to identify the closest proficiency level the training course is teaching.
#             Give your response in JSON format like this: {{'level': <>, 'reason': <>, 'confidence': <high | medium | low>}}"""
#         },
#         {
#             "role": "user",
#             "content": f"""You are given the training course content {crs_content}, it teaches the skill {skill}, which has performance expectations at different levels of proficiency according to {temp_kb_dic}.
#             Help me to identify the most appropriate proficiency level of the skill, that the training course is teaching.
#             Give your response in JSON format like this: {{'level': <proficiency level>, 'reason': <your reason for the output>, 'confidence': <high | medium | low>}}"""
#         }
#     ]
#     return system_message


def form_sys_msg(pe_kb_dic, crs_content, skill, skill_pl_reference_chart):
    temp_kb_dic = pe_kb_dic[skill]
    system_message = [
        {
            "role": "system",
            "content": """You are a helpful expert in the area of training courses and skills, you are helping me with associating the correct skills proficiency levels to training course contents, based on 2 sets of information:
            1. Knowledge Base that defines the knowledge and abilities that are associated with each skill at the various proficiency levels.
            2. Reference Document that defines the performance expectation for different proficiency levels.
            For each pair of course content and skill taught, identify the most appropriate proficiency level for the skill that's being taught by the training course using the Knowledge Base first. 
            Only when you need additional information, refer to the Reference Document for decision. Do not ever tag a skill with a proficiency level that's not found in the Knowledge Base.
            Give your response in JSON format like this: {{'proficiency': <>, 'reason': <>, 'confidence': <high | medium | low>}}""",
        },
        {
            "role": "user",
            "content": f"""For the training course {crs_content}, what is the most appropriate proficiency level to be tagged to the skill {skill}, based on the Knowledge Base {pe_kb_dic}, only if you need more information, refer to the Reference Document {skill_pl_reference_chart}.
            Give your response in JSON format like this: {{'proficiency': <>, 'reason': <>, 'confidence': <high | medium | low>}}""",
        },
    ]
    return system_message


def get_pl_tagging(row, id_list, pe_kb_dic, skill_pl_reference_chart):
    sys_msg = form_sys_msg(
        pe_kb_dic=pe_kb_dic,
        crs_content=row["course_text"],
        skill=row["TSC Title/Skill"],
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
    pe_kb_dic,
    skill_pl_reference_chart,
    checkpoint_filename,
):
    def process_result(row, id_list, pe_kb_dic, skill_pl_reference_chart):
        with open(checkpoint_filename, "a") as file:
            gpt_output = get_pl_tagging(
                row, id_list, pe_kb_dic, skill_pl_reference_chart
            )
            print(gpt_output[0])
            id_list.append(gpt_output[0])
            result_list.append(gpt_output[1])

            file.write(str(gpt_output[0]) + "\n")
        file.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
        for _, row in df.iterrows():
            executor.submit(
                process_result, row, id_list, pe_kb_dic, skill_pl_reference_chart
            )

        return id_list, result_list
