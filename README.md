# 🚀 Numoni MultiDB Intelligent Query System

An intelligent multi-database query routing system built for Numoni (Digidelight Company), designed to process natural language queries and intelligently route them across multiple MongoDB databases (Customer, Merchant, Authentication).

This system combines rule-based logic, fuzzy matching, and BERT-based semantic similarity to accurately interpret user intent and dynamically generate database queries.

---

## 🧠 Project Overview

This project was developed to support real-world financial/merchant-client workflows in Nigeria, where user queries need to be dynamically interpreted and routed to the correct database.

The system intelligently:

- Detects user intent from natural language
- Identifies relevant database (Customer / Merchant / Authentication)
- Extracts filters dynamically
- Applies semantic understanding using BERT
- Executes MongoDB queries safely
- Returns structured results

---

## 🏢 About Numoni

Numoni is a financial infrastructure platform focused on enabling seamless digital payment and merchant solutions. It supports merchant onboarding, transaction processing, customer management, and authentication workflows.

This project was developed as an intelligent query interpretation layer designed to support internal analytics and operational queries across multiple Numoni system databases.

The goal of this system is to:

- Interpret natural language queries from internal teams
- Route queries to the correct operational database
- Dynamically extract filters and constraints
- Construct safe MongoDB queries automatically

This makes internal data access faster, more intuitive, and less dependent on manual database querying.

---

## 🗄️ Database Architecture

The system connects to three independent MongoDB databases:

### 1️⃣ Customer Database (`numoni_customer`)

Contains customer-related collections such as:

- Customer profiles
- Account details
- KYC information
- Transaction summaries
- Status & metadata fields

Used for:
- Customer lookups
- Account status checks
- Region-based filtering
- Activity-based queries


---

### 2️⃣ Merchant Database (`numoni_merchant`)

Contains merchant-related operational data such as:

- Merchant registration details
- Business information
- Location data
- Merchant status
- Onboarding timestamps
- Activity metrics

Used for:
- Merchant registration analysis
- Region-based merchant filtering
- Onboarding reports
- Status validation queries


---

### 3️⃣ Authentication Database (`authentication`)

Contains authentication and access-related collections such as:

- User credentials (secured & hashed)
- Login activity
- Role-based access information
- Permission mapping
- Account verification data

Used for:
- Authentication checks
- Role validation
- Login activity analysis
- Access control reporting


---

## 🔁 Multi-Database Routing Logic

The system does not query all databases blindly.

Instead, it:

1. Analyzes the user’s natural language input
2. Detects the intended domain (Customer / Merchant / Authentication)
3. Routes the query only to the relevant MongoDB database
4. Extracts filters dynamically
5. Constructs structured MongoDB queries

This reduces unnecessary load and improves performance and accuracy.

---

## 🧠 Intelligent Query Interpretation

Unlike traditional dashboard systems that rely on predefined filters, this system:

- Understands free-form user queries
- Maps synonyms to schema fields
- Handles ambiguous phrasing
- Applies semantic similarity using BERT (in advanced mode)
- Falls back to fuzzy matching when needed

Example:

User Query:
> "Show merchants onboarded in Abuja after March 2024"

System Process:
- Detects domain → Merchant DB
- Identifies filters → location=Abuja, date > March 2024
- Maps to correct MongoDB fields
- Executes structured query

---

## 🔍 Why Multi-DB Matters

In real-world financial systems:

- Customer data
- Merchant data
- Authentication data

are separated for scalability and security reasons.

This project simulates a realistic production architecture where:

- Databases are logically separated
- Queries must be routed intelligently
- Schema structures differ across systems

---

## 🏗️ Code Architecture

The system is divided into four modular stages:

### 🔹 Part 1 – Intent & Database Detection
Determines which database the query belongs to:
- Customer DB
- Merchant DB
- Authentication DB

Uses:
- Keyword logic
- Fuzzy matching

---

### 🔹 Part 2 – Schema & Field Matching
Maps user language to database schema fields.
Handles:
- Field normalization
- Synonym resolution
- Partial matches

---

### 🔹 Part 3 – Query Construction Engine
Builds dynamic MongoDB queries using:
- Extracted filters
- Identified schema fields
- Logical operators

Ensures safe and optimized query structure.

---

### 🔹 Part 4 – Intelligent Filter Analysis

Two implementations:

#### 🔸 Rule-Based + Fuzzy Matching
- String similarity
- Heuristic-based interpretation

#### 🔸 BERT-Based Semantic Matching
- Context-aware similarity scoring
- Better handling of ambiguous queries
- Improved intent classification

---

## 🧩 Tech Stack

- **Python**
- **MongoDB**
- **BERT (Transformers)**
- **Groq LLM API**
- **FuzzyWuzzy / RapidFuzz**
- **dotenv for environment management**

---

## 🔐 Environment Setup

1. Clone the repository:

```bash
git clone https://github.com/jobin2201/numoni-mutlidb-system.git
cd numoni-mutlidb-system
```
2. Create your environment file:
```bash
cp .env.example .env
```
3. Add your credentials inside .env:
```bash
GROQ_API_KEY=your_groq_api_key_here
```
---



