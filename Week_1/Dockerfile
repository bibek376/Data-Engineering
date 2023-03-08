FROM python:3.9

RUN pip install pandas 

WORKDIR /app

#It will copy pipline.py file from local to docker /app/pipeline.py location

COPY pipeline.py pipeline.py

#To over ride python base prompt

#ENTRYPOINT [ "bash" ]

ENTRYPOINT [ "python","pipeline.py"]