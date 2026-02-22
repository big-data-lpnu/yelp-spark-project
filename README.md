# Yelp Spark Project

This project is a data analysis and machine learning project that uses the Yelp dataset to predict business ratings based on various features. The project is implemented using Apache Spark, a powerful distributed computing framework, to handle large-scale data processing and analysis.

## Dataset

The dataset used in this project is the Yelp dataset, which contains information about businesses, reviews, and users. The dataset is available for download from the [Yelp Dataset Challenge](https://www.yelp.com/dataset) website.

## Project Structure

The project is organized into the following directories:

- `artifacts/`: Contains the raw and processed data files. This directory is used to store the intermediate and final results of the data processing and analysis steps. Gitignored.
- `src/`: Contains the source code for the project, including data processing scripts, machine learning models, and evaluation scripts.
- `notebooks/`: Contains Jupyter notebooks for exploratory data analysis and model development.
- `tests/`: Contains unit tests for the project to ensure the correctness of the code.

## Project Members

- [Oleksii Horyshevskyi](https://github.com/rojikaru)
- [Danylo Shliakhetko](https://github.com/shliakhetko)
- [Illia Matsko](https://github.com/illiamatsko)
- [Solomiia Trush](https://github.com/Ssomkk)

## Responsibilities

TBA

## Requirements

- Python 3.14 or higher
- Apache Spark

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
    uv sync
    ```

3. Set up a virtual environment and activate it:

    ```bash
    uv venv
    source .venv/bin/activate # On Windows, use .venv\Scripts\activate
    ```

4. Download the Yelp dataset from the [Yelp Dataset Challenge](https://www.yelp.com/dataset) website and place the files in the `artifacts/` directory.

    ```bash
    uv run -m src.artifacts.download
    ```

## Usage

Local execution:

```bash
uv run -m src.main
```

Via Docker:

```bash
# Build the Docker image
docker build -t yelp-spark-project .

# Execute
docker run -v $(pwd)/artifacts:/app/artifacts yelp-spark-project
```

## License

Please see the [UNLICENSE](UNLICENSE) file for details on the license for this project.
