# Yelp Spark Project

This project is a data analysis and machine learning project that uses the Yelp dataset to predict business ratings based on various features. The project is implemented using Apache Spark, a powerful distributed computing framework, to handle large-scale data processing and analysis.

## Dataset

The dataset used in this project is the Yelp dataset, which contains information about businesses, reviews, and users. The dataset is available for download from the [Yelp Dataset Challenge](https://www.yelp.com/dataset) website.

## Project Structure

The project is organized into the following directories:

- `artifacts/`: Contains the raw and processed data files. This directory is used to store the intermediate and final results of the data processing and analysis steps. Gitignored.
- `results/`: Contains the output of analysis transformations (CSV/Parquet exports). Gitignored.
- `src/`: Contains the source code for the project, organized into the following sub-modules:
  - `src/artifacts/`: Dataset download & unpacking automation scripts.
  - `src/analysis/`: Data validation and business analytics transformations.
    - `src/analysis/transformations/`: Topic-specific transformation modules (`business.py`, `engagement.py`, `review.py`, `user.py`).
  - `src/ml/`: Machine learning stage — user-level feature table, Spark ML preprocessing pipeline, regression (number of fans) and classification (elite users) models, evaluation and plots.
  - `src/preprocessing/`: ETL pipeline — data cleaning, flattening, EDA, and column lineage tracking.
  - `src/schemas/`: Dataset schema definitions for all Yelp entities.
  - `src/spark/`: Spark session factory and configuration helpers.
  - `src/utils/`: Shared utility functions.
  - `src/reports/`: Generated PDF reports and presentation.
    - `src/reports/ml/`: Machine learning stage report (Ukrainian), figures and metric tables.
  - `src/notebooks/`: Jupyter notebooks for exploratory data analysis, preprocessing comparison, transformation walkthroughs and the ML stage (`ml_models.ipynb`).
- `tests/`: Unit tests (`uv run pytest`).

## Project Members

- [Oleksii Horyshevskyi](https://github.com/rojikaru)
- [Danylo Shliakhetko](https://github.com/shliakhetko)
- [Illia Matsko](https://github.com/illiamatsko)
- [Solomiia Trush](https://github.com/Ssomkk)

## Responsibilities

1. Oleksii Horyshevskyi:
   - Setting up the repository.   
   - Creating the Dockerfile and image.   
   - Developing the Spark session module.   
   - Leading the transformation stage for user analysis.   

2. Danylo Shliakhetko:
   - Conducting the initial analysis of the Yelp dataset.   
   - Creating the notebook for testing.   
   - Leading the transformation stage for engagement analysis.   

3. Illia Matsko:
   - Formatting the overall project structure.   
   - Designing Spark schemas for the tables.   
   - Leading the transformation stage for business analysis.   

4. Solomiia Trush:
   - Configuring branches and the .gitignore file.   
   - Implementing the data loading functionality.   
   - Leading the transformation stage for review analysis. 

## Requirements

- Python 3.14 or higher
- Apache Spark (via [PySpark](https://pypi.org/project/pyspark/) ≥ 4.1.1)
- Java 17 or newer (see [Java Installation](#java-installation) below)
- [uv](https://docs.astral.sh/uv/) — fast Python package and project manager
- Key Python dependencies (managed by `uv`):
  - `pyspark` — distributed data processing
  - `pandas` — local data manipulation
  - `requests` + `tqdm` — dataset download with progress reporting
  - `black`, `flake8`, `pyright` — code quality tools
  - `pre-commit` — Git hook framework
  - `nbconvert` — Jupyter notebook conversion

## Installation

To set up the project, follow these steps:

1. Clone the repository:

    ```bash
    git clone git@github.com:big-data-lpnu/yelp-spark-project.git
    cd yelp-spark-project
    ```

2. Install [uv project manager](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer) and project dependencies:

    ```bash
    pip install uv
    ```

3. Set up a virtual environment and activate it:

    ```bash
    uv venv
    source .venv/bin/activate # On Windows, use .venv\Scripts\activate
    ```

4. Install the project dependencies:

    ```bash
    uv sync
    ```

5. Install pre-commit hooks (optional but recommended for contributors):

    ```bash
    uv run pre-commit install
    ```

6. Download the Yelp dataset from the [Yelp Dataset Challenge](https://www.yelp.com/dataset) website and place the files in the `artifacts/` directory.

    ```bash
    uv run -m src.artifacts.download
    ```

### Java Installation

Java 17 is required to run Apache Spark. You can install it manually or use [mise](https://mise.jdx.dev/) for automatic version management:

```bash
# Install mise (see https://mise.jdx.dev/getting-started.html)
mise install   # reads .java-version / mise.toml and installs Java 17 automatically
```

Alternatively, install Java 17 directly from [Eclipse Temurin](https://adoptium.net/) or your OS package manager.

## Usage

Local execution:

```bash
uv run -m src.main
```

Machine learning stage (user level: regression of the number of fans, classification of elite users; 3 models each). The full run is the notebook (~27 min on 8 cores; writes figures and tables to `src/reports/ml/`):

```bash
uv run jupyter nbconvert --to notebook --execute --inplace src/notebooks/ml_models.ipynb
```

The same pipeline as a script (`--quick` for a ~4 min smoke run into `results/ml/quick/`):

```bash
uv run -m src.ml.run --quick
```

The write-up is in [`src/reports/ml/README.md`](src/reports/ml/README.md).

Unit tests:

```bash
uv run pytest
```

Via Docker:

```bash
# Build the Docker image
docker build -t yelp-spark-project .

# Grant permissions to the results directory
mkdir -p results && sudo chmod o+w results

# Execute (Linux / macOS)
docker run -v $(pwd)/artifacts:/app/artifacts -v $(pwd)/results:/app/results yelp-spark-project

# Execute (Windows PowerShell)
docker run -v ${PWD}/artifacts:/app/artifacts -v ${PWD}/results:/app/results yelp-spark-project
```

## Development

This project uses the following tools to enforce code quality:

- **[black](https://black.readthedocs.io/)** — opinionated code formatter (line length: 80)
- **[flake8](https://flake8.pycqa.org/)** — style and error linter
- **[pyright](https://github.com/microsoft/pyright)** — static type checker
- **[pre-commit](https://pre-commit.com/)** — runs checks automatically on each commit

Run all checks manually:

```bash
# Format code
uv run black .

# Lint
uv run flake8 .

# Type-check
uv run pyright

# Run all pre-commit hooks against all files
uv run pre-commit run --all-files
```

## License

Please see the [UNLICENSE](UNLICENSE) file for details on the license for this project.
