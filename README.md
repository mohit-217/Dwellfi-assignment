<p align="center">
  <img src="https://img.icons8.com/?size=512&id=55494&format=png" width="20%" alt="DWELLFI-ASSIGNMENT-logo">
</p>
<p align="center">
    <h1 align="center">DWELLFI-ASSIGNMENT</h1>
</p>
<p align="center">
		<em>Built with the tools and technologies:</em>
</p>
<p align="center">
	<img src="https://img.shields.io/badge/Python-3776AB.svg?style=flat&logo=Python&logoColor=white" alt="Python">
	<img src="https://img.shields.io/badge/Docker-2496ED.svg?style=flat&logo=Docker&logoColor=white" alt="Docker">
	<img src="https://img.shields.io/badge/FastAPI-009688.svg?style=flat&logo=FastAPI&logoColor=white" alt="FastAPI">
</p>

<br>

##### Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
    - [Prerequisites](#-prerequisites)
    - [Installation](#-installation)
    - [Usage](#-usage)
- [Environment Variables](#-environment-variables)
- [API Endpoints](#-api-endpoints)

---

##  Overview

<p> This FastAPI application provides a RESTful API with various endpoints for performing database operations, health checks, and versioning. The application includes CORS middleware for handling cross-origin requests and uses API key-based authentication for security.</p>

---

### Features

- FastAPI Based Authentication
- Docker Image support


---

##  Repository Structure

```sh
└── Dwellfi-assignment/
    ├── Dockerfile
    ├── README.md
    ├── app
    │   ├── __init__.py
    │   ├── classes
    │   └── main.py
    └── requirements.txt
```

##  Getting Started

###  Prerequisites

**Python**: `version 3.10.12`

###  Installation

Build the project from source:

1. Clone the Dwellfi-assignment repository:
```sh
❯ git clone https://github.com/mohit-217/Dwellfi-assignment
```

2. Navigate to the project directory:
```sh
❯ cd Dwellfi-assignment
```

3.1 Install the required dependencies:
```sh
❯ pip install -r requirements.txt
```
3.2 Or build the docker image (Need to update docker-compose.yaml) :
```sh
❯ docker image build -t dwellfi-app .
```
## Environment Variables

Set the following environment variables before running the application:

1. **`OPENAI_API_KEY`**: OpenAI API key (e.g., `sk-XXXXXXXXXXXXXXXXXXXXXX`)
2. **`DB_Connection_url`**: Database connection URL (e.g., `postgresql://user:password@localhost:5432/mydatabase`)
3. **`Dwellfi_key`**: Custom key for Dwellfi authentication (e.g., `your_dwellfi_key_here`)
4. **`API_KEY`**: API key for authenticating requests (e.g., `your_api_key_here`)


###  Usage

To run the project using uvicorn change .classes to classes and run below command:
Create a .env file on the level of app directory:
```sh
❯  .env copy all environment variables
```

Navigate to the project directory:
```sh
❯ cd Dwellfi-assignment/app
```
```sh
❯ uvicorn main:app
```
To run the project using Docker Run the below command (need to update docker-compose.yaml)
```sh
❯ docker container run --publish 8080:8080 --name dwellfi-app-container dwellfi-app
```

## API Endpoints

### 1. Root Endpoint
- **Endpoint:** `/`
- **Method:** `GET`
- **Parameters:**
  - `api_key` (string): The API key provided in the request header for authentication.
- **Description:** Returns a welcome message if the provided API key is valid.
- **Response:** 
  - **Success:** Returns a JSON response with a welcome message. 
    ```json
    {
      "message": "Welcome!"
    }
    ```
  - **Error:** Returns an error message with status code `403` if the API key is invalid.

### 2. Health Check
- **Endpoint:** `/health`
- **Method:** `GET`
- **Parameters:**
  - `api_key` (string): The API key provided in the request header for authentication.
- **Description:** Returns the health status of the API.
- **Response:**
  - **Success:** Returns a JSON response with the health status. 
    ```json
    {
      "status": "healthy"
    }
    ```
  - **Error:** Returns an error message with status code `403` if the API key is invalid.

### 3. Version
- **Endpoint:** `/version`
- **Method:** `GET`
- **Parameters:**
  - `api_key` (string): The API key provided in the request header for authentication.
- **Description:** Returns the current version of the API.
- **Response:**
  - **Success:** Returns a JSON response with the API version.
    ```json
    {
      "version": "1.0.0"
    }
    ```
  - **Error:** Returns an error message with status code `403` if the API key is invalid.

### 4. Perform Database Operation
- **Endpoint:** `/perform_db_operation`
- **Method:** `POST`
- **Parameters:**
  - `user_query` (string, form data): The user query to be executed on the database.
  - `api_key` (string): The API key provided in the request header for authentication.
- **Description:** Executes a database operation based on the provided user query.
- **Response:**
  - **Success:** Returns the result of the database operation.(Read,Delete,Update or Insert record)
  - **Error:** "Error Encounterd View stacktrace for detailed Explanation

### 5. Check Latest Database Table Record
- **Endpoint:** `/check_database_table`
- **Method:** `POST`
- **Parameters:**
  - `api_key` (string): The API key provided in the request header for authentication.
- **Description:** Retrieves the latest record from the database table.
- **Response:**
  - **Success:** Returns a JSON response with the latest record from the database.
  - **Error:** Error Encounterd View stacktrace for detailed Explanation






