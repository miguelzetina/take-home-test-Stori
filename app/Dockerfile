FROM public.ecr.aws/lambda/python:3.7

COPY . .

RUN python3.7 -m pip install --upgrade pip && python3.7 -m pip install -r requirements.txt -t .

CMD ["process_txns_file.handler"]