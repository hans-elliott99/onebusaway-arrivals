FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy files 
COPY ./requirements.txt /app/
COPY ./.env_docker /app/.env
COPY ./app/src/ /app/src/

RUN mkdir -p /app/data

# RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install -r /app/requirements.txt

# https://stackoverflow.com/questions/59812009/what-is-the-use-of-pythonunbuffered-in-docker-file
ENV PYTHONUNBUFFERED=1

# run the application
CMD ["python", "src/main.py"]

