FROM postgres:16-alpine
RUN apk add --no-cache python3
WORKDIR /runner
COPY runner.py guardian.sql payment.sql attendance.sql finance.sql /runner/
ENTRYPOINT ["python3", "-u", "/runner/runner.py"]
