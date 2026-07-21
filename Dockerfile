FROM python:3.11-slim

# Hugging Face Spaces containers run as a non-root user by convention.
RUN useradd -m -u 1000 user
USER user
ENV PATH="/home/user/.local/bin:$PATH"

WORKDIR /app

COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

COPY --chown=user ingest/ ingest/
COPY --chown=user sync/ sync/
COPY --chown=user webapp/ webapp/

ENV PYTHONUNBUFFERED=1
EXPOSE 7860

CMD ["python3", "-m", "webapp.app"]
