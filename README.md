# 🗂️ Distributed MapReduce System on AWS

This project implements a **distributed MapReduce system** in **Go (Golang)**, using  
**Amazon S3** for distributed storage, **Amazon ECR** for container image hosting,  
and **Amazon ECS with Fargate** for running containerized Splitter, Mapper, and Reducer tasks.

---

## System Architecture
<img width="561" height="546" alt="image" src="https://github.com/user-attachments/assets/7dc1949d-4f23-47d0-a47f-7a82cf5bdff6" />


---
### 🧩 Workflow Summary
1. **Dockerize** the Go service for Splitter, Mapper, and Reducer.  
2. **Push** the image to **Amazon ECR** (Elastic Container Registry).  
3. **Create an ECS Cluster** using **AWS Fargate** (serverless container execution).  
4. **Run ECS Tasks** for each role:
   - One Splitter Task
   - Multiple Mapper Tasks (can scale horizontally)
   - One Reducer Task
5. Each Task communicates through **public endpoints** or via the same **ECS VPC network**.
6. **S3** is used for all data exchange between tasks.

---

## 🧱 Project Structure
<img width="635" height="216" alt="image" src="https://github.com/user-attachments/assets/2f798850-2866-47a5-a214-d27877c76965" />

---

## ⚙️ Features

✅ **Distributed Execution** – Splitter, Mapper, and Reducer run on separate AWS instances.  
✅ **Scalable Parallelism** – Configure number of mappers (`PARTS` variable in `test.sh`).  
✅ **AWS S3 Integration** – Handles input, intermediate, and final outputs through S3.  
✅ **REST APIs** – Each role exposes endpoints (`/split`, `/map`, `/reduce`) via Gin.  
✅ **Automated Pipeline** – Single `bash test.sh` orchestrates the entire process.  
✅ **Container-Ready** – Deployable via Docker or AWS Fargate.

---

## 🧰 Technologies Used

| Component         | Technology          |
|-------------------|---------------------|
| Language          | Go (Golang)         |
| Web Framework     | Gin                 |
| Cloud Storage     | Amazon S3           |
| Compute           | **ECS + Fargate**   |
| SDK               | AWS SDK for Go v2   |
| Container Registry| **ECR**             |
| Script Automation | Bash                |
| Containerization  | Docker              |
| Deployment        | **Docker + AWS CLI**|

---

## Run Instructions

#### (1) Build and Push Docker Image
docker buildx build --platform linux/amd64 -t $ECR_URL:mr -f Dockerfile --push .

#### (2) Create ECS Tasks
Splitter Task → MODE=splitter
Mapper Tasks → MODE=mapper
Reducer Task → MODE=reducer

Assign each task a Public IP and security group allowing inbound port 8080.

#### (3) Run MapReduce
Edit test.sh and fill in:
SPLITTER_IP="54.xxx.xx.xx"
MAPPER_IPS=("35.xxx.xx.xx" "54.xxx.xx.xx" "3.xxx.xx.xx")
REDUCER_IP="3.xxx.xx.xx"


#### （4）Run the Full Workflow from Local or Splitter Node

chmod +x test.sh
./test.sh


## Example output:
=== Split phase (3 parts) ===
Split time: 353 ms
=== Map phase ===
Map time: 18 ms
=== Reduce phase ===
Reduce time: 366 ms
=== Summary ===
Split:  353 ms
Map:    18 ms
Reduce: 366 ms
TOTAL:  737 ms

---

## Example Output (final.json)

{
  "and": 1045,
  "the": 1321,
  "king": 88,
  "hamlet": 42
}

---

## Author

**Yuxin Hu**  
Master of Science in Computer Science  
**Northeastern University – Silicon Valley**  
📧 hu.yuxin3@northeastern.edu  
---

## 🧩 License

This project is for educational and research purposes.  
© 2025 Yuxin Hu. All rights reserved.
