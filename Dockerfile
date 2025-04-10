# We use the OpenJDK image due to Spark's reliance on it
FROM openjdk:11-jre-slim

# Install Python & CURL to get Poetry, then install Poetry
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    curl \
    git \
    procps \
    && apt-get clean
RUN curl -sSL https://install.python-poetry.org | python3 -

# Define paths for Poetry and the working directory
ENV PATH="/root/.local/bin:$PATH"
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Create working directory & create the virtual poetry environment
WORKDIR /app
COPY pyproject.toml poetry.lock* ./
RUN poetry install --without dev

# Copy project data
COPY docker .

# Launch the analysis script via the virtual Poetry environment
CMD ["poetry", "run", "python3", "scripts/run_analysis.py"]
