## Project Documentation
The project leverages GenAI for automated course to skills tagging (at the proficiency levels). It has 2 parts to it:


###################
UPDATE: Ignore the R1 & R2 processing / output folders, just use the src folder for reference.
###################

### Part I
Using SSG Skills Framework as the referenced knowledge base for TSCs.
Input to Large Language Model includes:
- TSC Title
- TSC Proficiency Level Description
- Corresponding Knowledge
- Corresponding Abilities

Using TGS Training Course data.
Input to Large Language Model includes:
- Course Title
- What You'll Learn
- About This Course

Using SEA for Skills (without Proficiency Level) Extraction.
Input to Large Language Model includes:
- Skill Title

Output from GenAI Model:
For each skill associated with each training course, the output will indicate the most appropriate proficiency level of the skill, that is being taught by the course.
- proficiency level (integer between 1-6, depending on PL availability of skill in SFw)
    - If LLM is unable to find any suitable PL to associate with the TSC, a PL == 0 will be returned
- reason (LLM's reason for the predicted PL)
- confidence level (LLM's confidence of the predicted output: High / Medium / Low)

Not all TSCs extracted from all training courses can be tagged with a PL. Hence, a Part II processing is required.

### Part II
Using SSG TSC Responsibility-Autonomy-Complexity Chart (SSG TSC RAC Chart) to tag skills that did not have any PL association after Part I processing.
Input to Large Language Model includes:
- Reformatted dictionary of SSG TSC RAC Chart
- TSC Title
- Corresponding Knowledge
- Corresponding Abilities

Output from GenAI Model:
- proficiency level (integer between 1-6, depending on PL availability of skill in SFw)
- reason (LLM's reason for the predicted PL)
- confidence level (LLM's confidence of the predicted output: High / Medium / Low)

At the end of Part II processing, all TSCs extracted from the training courses will have a PL tagged to it.

However, for TSCs extracted from training courses, that do not have any data on "What You'll Learn" and "About This Course", those TSCs were not be able to be processed for PL tagging during this exercise.

A total of 5,187 courses had information that are required and their 6,950 corresponding TSCs extracted have been tagged with PL information under this project.
