# dlt-client
DLT Client is a MVP Data Engineering project which aims to provide a capabilities of building ETL pipelines using Web interface and leveraging Low/Zore code features.

In terms of stack the front end is build in <a href="https://stilljs.dev/">Still.js</a> framework and the Backend is built in <a href="https://dlthub.com/">dlt-hub (Python)</a>. This is a single repository comprising both the backend and the UI.

<br>

##### Table of Contents  
- [Set up and Running with Docker](#docker-setup)  
    - [Prerequisites (Docker compose)](#prerequisites)  
    - [Running the project (Docker compose)](#running-the-project)  
    - [Project Structure Overview (Docker compose)](#project-structure-overview)  
- [Setup without docker](#emphasis)
[Architecture](#headers)
[Design System](#headers)



<a name="docker-setup"></a>

## Set up and Running with Docker

This project uses docker to orchestrate services such as Backend and Frontend.

<br>

<a name="prerequisites"></a>

### Prerequisites


Make sure you have the following installed:

- [Docker](https://www.docker.com/products/docker-desktop)
- [Docker Compose](https://docs.docker.com/compose/)

You can verify the installation with:

```bash
docker --version
docker-compose --version
```

<br>

<a name="running-the-project"></a>

### 🚀 Running the Project

```bash
docker compose up --build
```
<br>

### 💬 Notes
- You'll access the ui from http://localhost:8080


<br>

<a name="project-structure-overview"></a>

### 🧱 Project Structure Overview

```js
.
├── docker-compose.yml
├── backend/
│   └── Dockerfile
│   └── src
│       └── ...│
├── ui/
│   └── Dockerfile
│   └── app
│       └── ...
└── README.md
```

<br><hr>

## Architecture

### Overview

<img src="assets/architecture.png">

<br>
<br>

### Design System

<img src="assets/dsistem.png">


<br><hr>