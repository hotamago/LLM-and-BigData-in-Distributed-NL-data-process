# <span style="color:#2e6c80">LLM and BigData in Process NL Data from Internet</span>

![Build Status](https://img.shields.io/badge/build-passing-brightgreen) ![License](https://img.shields.io/badge/license-MIT-blue)

## <span style="color:#2e6c80">Table of Contents</span>

1. [Installation](#installation)
2. [Architecture](#architecture)
3. [Project Information](#project-information)
4. [Active Stage Projects](#active-stage-projects)
5. [Authors](#authors)

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/hotamago/LLM-and-BigData-in-Distributed-NL-data-process.git
   ```
2. Navigate to the project directory:
   ```bash
   cd LLM-and-BigData-in-Distributed-NL-data-process
   ```
3. Run the Docker setup script (Must have docker to run):
   ```bash
   cd docker
   ./run.cmd
   ```
4. Access the application at `http://localhost:3005`.

## Architecture
![Docker Compose Visualization](docker/docker-compose.VizFormats.png)

## <span style="color:#2e6c80">Project Information</span>
Big Data and LLM Integration for processing Natural Language Data sourced from the internet. This project utilizes distributed storage and processing frameworks like Hadoop and Spark, orchestrated through Docker Compose, to efficiently manage and analyze large-scale datasets using Language Learning Models.

## Active Stage Projects
### Stage 1: Generate Queries
Leverage a Large Language Model (LLM) to generate search keywords based on the provided input requirements.

### Stage 2: Get URLs
Utilize a search engine's API to query the generated keywords and collect a list of relevant URLs associated with those keywords.

### Stage 3: Get Content
Employ multiple Spark nodes to scrape content from the collected URLs. Store the extracted data, formatted as raw text, into Hadoop HDFS for further processing.

### Stage 4: Generate Columns Info
Use the LLM to create metadata for data columns based on user-defined input requirements. This ensures consistency and standardization during the distributed data processing phase across multiple nodes.

### Stage 5: Process Data
Input data, consisting of natural language text retrieved from web sources, is processed using Spark nodes. The LLM on each node transforms the raw text into structured data according to the column metadata specifications created in step 4.
The processed, structured data is then saved to Hadoop HDFS for subsequent analysis.

### Stage 6: Final Processing
The LLM generates Python scripts tailored to the specific data processing requirements, using the structured data from step 5 as input.
Execute the generated Python scripts in a distributed Spark environment to perform advanced data processing tasks, such as data aggregation and final output generation. Deliver the processed results in the required format to meet the predefined objectives.

## Workflow
![Workflow](workflow.png)

## Authors
- **Nguyễn Hoàng Sơn**  
  Email: [nhson.ceo@hotavn.com](mailto:nhson.ceo@hotavn.com)  
  [Facebook](https://www.facebook.com/HotaVN/) | [LinkedIn](https://www.linkedin.com/in/hotamago/)
