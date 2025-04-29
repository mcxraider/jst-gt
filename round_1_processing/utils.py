from openai import OpenAI
from threading import Lock
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import json

def get_openai_client(api_key, base_url):
    client = OpenAI(api_key=api_key,base_url=base_url)
    return client

def get_skill_info(skill_title: str, skill_df: pd.DataFrame) -> dict:
    """Function filters for skill_title"""
    skill_info = skill_df[skill_df['TSC_CCS Title'] == skill_title] # extracts all dataframe with the skill title
    proficiency_dict = {}
    levels = skill_info['Proficiency Level'].unique() # get the all proficiency levels of skill
    
    for level in levels:
        level_info = skill_info[skill_info['Proficiency Level'] == level]  #extracts all dataframes with the proficiency level
        knowledge_items = level_info[level_info['Knowledge / Ability Classification'] == 'knowledge']['Knowledge / Ability Items'].unique().tolist() # extract all knowledge of proficiency into skill
        ability_items = level_info[level_info['Knowledge / Ability Classification'] == 'ability']['Knowledge / Ability Items'].unique().tolist() # extract all ability of proficiency into skill
        proficiency_description = level_info['Proficiency Description'].iloc[0] #extract proficiency description
        proficiency_dict[level] = {
            'knowledge': knowledge_items,
            'ability': ability_items,
            'proficiency_description': proficiency_description
        } 
    
    return proficiency_dict

def format_for_openai(proficiency_info: dict, setup: int) -> str:
    """Function formats data for OpenAI prompt"""
    formatted_data = ""
    
    for level, items in proficiency_info.items():   #format data for LLM parsing
        formatted_data += f"Proficiency Level: {level}\n"
        formatted_data += f"Proficiency Description: {items['proficiency_description']}\n"
        formatted_data += "Knowledge Items:\n"
        if setup == 1 or setup == 3:
            for item in items['knowledge']:
                formatted_data += f"- {item}\n"
        if setup == 2 or setup == 3:
            formatted_data += "Ability Items:\n"
            for item in items['ability']:
                formatted_data += f"- {item}\n"
        formatted_data += "\n"
    return formatted_data

def get_proficiency_level(skill_title: str, skill_info: dict, course_description: str, course_title: str, course_learning: str, setup: int, client) -> str:
    """Function to call OpenAI API. Setup is called based on the classification type (1,2,3)"""
    formatted_data = format_for_openai(skill_info, setup)
    sys_messages = [
        {"role": "system", 
         "content": f"""
         ROLE: You are an expert analyst on educational courses
         CONTEXT:
         You are given the knowledge and abilities required of each proficiency level of {skill_title}, {formatted_data}.
         You will be required to give one of the proficiency level based on information given to you on a course.
         GIVEN INFORMATION:
                You are given a description of the course
        TASK: 
                1. Analyse the given course description.

                2. Classify the course to one of the proficiency level given. Give 0 if the course definitely does not fall in any proficiency level of {skill_title}.
                   Evaluate the categorization based on the course description. IF YOU ARE UNSURE, CATEGORIZE THE COURSE UNDER "0".

                3. Provide a reason, in less than 30 words, of how you arrived at your conclusion.

                4. Provide a confidence of your categorisation: low / medium / high. Choose only from these 3 words and do not return anything other than these 3 words.

                OUTPUT FORMAT:
                Return me ONLY ONE DICTIONARY of format
                {{
                    "proficiency_level": "xxx", 
                    "reason": "xxx", 
                    "confidence": "xxx"
                }}
                
                DO NOT RETURN ANYTHING OTHER THAN THIS DICTIONARY. YOUR OUTPUT IS MEANT TO BE PARSED BY ANOTHER COMPUTER PROGRAM.
                """},
        {"role": "user", "content": f"""Determine the appropriate proficency level based on the following description of a course: {course_title}, {course_description}, with the following course learning: {course_learning}.
        """}
    ]
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages= sys_messages
    )
    
    completion_output = response.choices[0].message.content
    return completion_output


def process_row(row, skill_info_dict, pe_knowledge_df, lock, client):
    skill_title = row["TSC Title/Skill"]
    course_title = row["Course Title"]
    course_description = row["About This Course"]
    course_learning = row["What You'll Learn"]
    
    with lock:
        if skill_title in skill_info_dict:
            proficiency_info = skill_info_dict[skill_title]
        else:
            proficiency_info = get_skill_info(skill_title, pe_knowledge_df)
            skill_info_dict[skill_title] = proficiency_info
    
    proficiency_level_with_reason = get_proficiency_level(skill_title, proficiency_info, course_description, course_title, course_learning, 3, client)
    # print(proficiency_level_with_reason, type(proficiency_level_with_reason))
    try:
        res_dict = json.loads(proficiency_level_with_reason)
    except Exception as e:
        print(proficiency_level_with_reason, e)
        res_dict = json.loads(get_proficiency_level(skill_title, proficiency_info, course_description, course_title, course_learning, 3, client))
    res_dict["Skill"] = row["TSC Title/Skill"]
    res_dict["Course Ref Number"] = row["Course Reference Number"]
    
    #with progress_lock:
        #progress_counter[0]+=1
        #print(f"Completed {progress_counter[0]}/{total}")
    return res_dict

def run_in_parallel(course_df, pe_knowledge_df, client):
    skill_info_dict = {}
    results = []
    lock = Lock()
    #progress_lock = Lock()
    #progress_counter = [0]
    #total_rows = len(course_df)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_row, row, skill_info_dict, pe_knowledge_df, lock, client) for ind, row in course_df.iterrows()]
        for future in futures:
            results.append(future.result())
    results_df = pd.DataFrame(results)
    return results_df