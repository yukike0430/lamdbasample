FROM lambci/lambda:build-python3.7
ENV LANG C.UTF-8
ENV AWS_DEFAULT_REGION ap-northeast-1
 
ADD . .
 
CMD pip install -r requirements.txt -t /var/task && \
  zip -r9 lambda_function.zip ./*