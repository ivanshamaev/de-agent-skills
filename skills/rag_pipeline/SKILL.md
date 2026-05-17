---
name: rag-data-pipeline
description: Use when designing, building, or debugging a RAG (Retrieval-Augmented Generation) data pipeline — document ingestion, chunking strategies (fixed/recursive/semantic/structure-aware), embedding models (OpenAI/Cohere/sentence-transformers), vector stores (pgvector/Chroma/Qdrant/Weaviate), incremental refresh, hybrid retrieval (dense + BM25 + RRF), re-ranking with cross-encoders, metadata filtering, and production monitoring.
---

# RAG Data Pipeline

## When to Use

Load this skill when the user needs to:
- Design or implement end-to-end RAG ingestion and retrieval pipelines
- Choose or implement a chunking strategy (fixed-size, recursive, semantic, document-structure-aware)
- Embed documents with OpenAI, Cohere, or local sentence-transformers
- Set up or query vector stores: pgvector, Chroma, Qdrant, or Weaviate
- Build incremental embedding refresh pipelines (hash-based change detection, Airflow orchestration)
- Implement hybrid retrieval (dense vector + BM25/keyword + RRF fusion)
- Add cross-encoder re-ranking to improve retrieval precision
- Filter vector search results with metadata predicates
- Monitor retrieval quality (hit rate, MRR, latency) in production

---

## RAG Architecture Overview

A production RAG system has two distinct flows: an **offline indexing pipeline** and an **online query pipeline**.

```
OFFLINE INDEXING PIPELINE
==========================

 Raw Documents
 (PDF, HTML, MD, DB rows)
       |
       v
 [Document Loader]          langchain DocumentLoaders / custom readers
       |
       v
 [Chunker]                  fixed-size | recursive | semantic | structure-aware
       |
       v
 [Deduplication / Hash]     SHA-256 of chunk text → skip if already indexed
       |
       v
 [Embedding Model]          OpenAI / Cohere / sentence-transformers (batched)
       |
       v
 [Vector Store Upsert]      pgvector / Qdrant / Weaviate / Chroma
  + metadata (doc_id,        (chunk_id as upsert key)
    source, section,
    date, hash)
       |
       v
 [BM25 Index Update]        Elasticsearch / OpenSearch / rank_bm25 (sparse)


ONLINE QUERY PIPELINE
=====================

 User Query
       |
       +---> [Query Embedder] ------> [Vector (Dense) Search] ---+
       |                                                          |
       +---> [BM25 / Keyword Search] ---------------------------> +
                                                                  |
                                                          [RRF Fusion]
                                                                  |
                                                     [Cross-Encoder Reranker]
                                                       (optional, top-100→top-5)
                                                                  |
                                                          [Top-k Chunks]
                                                                  |
                                                       [LLM Generation]
                                                   (context + system prompt)
                                                                  |
                                                           Final Answer
```

---

## Chunking Strategies

### 1. Fixed-Size with Overlap

Simple baseline. Use when documents have no reliable structure.

```python
from langchain.text_splitter import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(
    chunk_size=512,           # tokens approximated by chars; tune to embedding model limit
    chunk_overlap=64,         # ~12% overlap prevents boundary cuts
    length_function=len,
    separators=["\n\n", "\n", ". ", " ", ""],
)

chunks = splitter.split_text(document_text)
```

Rule of thumb: chunk_size 512–1024 chars, overlap 10–20% of chunk_size. Larger chunks increase recall; smaller chunks improve precision.

### 2. Recursive Character Splitting (recommended default)

Tries each separator in order; falls back to the next only when the chunk is still over the limit. Respects paragraph → sentence → word boundaries.

```python
from langchain.text_splitter import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=150,
    separators=["\n\n", "\n", "(?<=\\. )", " ", ""],
)
docs = splitter.create_documents(
    texts=[page_content],
    metadatas=[{"source": "report_2024.pdf", "page": 3}],
)
```

### 3. Semantic Chunking (sentence-boundary)

Groups sentences until adding the next sentence would push cosine distance past a threshold. Best for narrative prose.

```python
from langchain_experimental.text_splitter import SemanticChunker
from langchain_openai import OpenAIEmbeddings

semantic_splitter = SemanticChunker(
    embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
    breakpoint_threshold_type="percentile",   # or "standard_deviation"
    breakpoint_threshold_amount=95,           # split when similarity drops below 95th pct
)
docs = semantic_splitter.create_documents([document_text])
```

Custom implementation without LangChain dependency:

```python
import re
import numpy as np
from sentence_transformers import SentenceTransformer

def semantic_chunk(text: str, model_name: str = "all-MiniLM-L6-v2", threshold: float = 0.75) -> list[str]:
    model = SentenceTransformer(model_name)
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    if len(sentences) <= 1:
        return sentences

    embeddings = model.encode(sentences, normalize_embeddings=True)
    # cosine similarity between adjacent sentences
    similarities = [
        float(np.dot(embeddings[i], embeddings[i + 1]))
        for i in range(len(embeddings) - 1)
    ]

    chunks, current = [], [sentences[0]]
    for i, sim in enumerate(similarities):
        if sim < threshold:
            chunks.append(" ".join(current))
            current = [sentences[i + 1]]
        else:
            current.append(sentences[i + 1])
    if current:
        chunks.append(" ".join(current))
    return chunks
```

### 4. Document-Structure-Aware Chunking

Splits on Markdown headers or HTML tags, preserving section context. Best for documentation, wikis, knowledge bases.

```python
from langchain.text_splitter import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter

# Step 1: split on Markdown headers to get sections with header metadata
header_splitter = MarkdownHeaderTextSplitter(
    headers_to_split_on=[
        ("#", "h1"),
        ("##", "h2"),
        ("###", "h3"),
    ],
    strip_headers=False,  # keep headers in chunk text for context
)
header_splits = header_splitter.split_text(markdown_text)

# Step 2: further split large sections with recursive splitter
char_splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=80)
final_chunks = char_splitter.split_documents(header_splits)
# Each chunk retains {"h1": "...", "h2": "...", "h3": "..."} metadata

# HTML-aware chunking
from langchain.text_splitter import HTMLHeaderTextSplitter

html_splitter = HTMLHeaderTextSplitter(
    headers_to_split_on=[("h1", "h1"), ("h2", "h2"), ("h3", "h3")]
)
html_splits = html_splitter.split_text(html_content)
```

---

## Embedding Models

### OpenAI text-embedding-3-small / text-embedding-3-large

```python
import time
import hashlib
from openai import OpenAI

client = OpenAI()  # reads OPENAI_API_KEY from env

def embed_batch_with_retry(
    texts: list[str],
    model: str = "text-embedding-3-small",  # 1536-dim; use "large" for 3072-dim
    batch_size: int = 100,
    max_retries: int = 5,
) -> list[list[float]]:
    """Batch embed with exponential backoff for rate limiting."""
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        for attempt in range(max_retries):
            try:
                resp = client.embeddings.create(input=batch, model=model)
                all_embeddings.extend([r.embedding for r in resp.data])
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                wait = 2 ** attempt
                time.sleep(wait)
    return all_embeddings
```

**Dimension considerations:**
- `text-embedding-3-small`: 1536 dims, $0.02/1M tokens — best cost/quality for most RAG use cases
- `text-embedding-3-large`: 3072 dims, $0.13/1M tokens — higher recall, larger index size
- Both support Matryoshka truncation: pass `dimensions=256` to get a shorter vector with acceptable quality loss

### Cohere Embeddings

```python
import cohere

co = cohere.Client()  # reads COHERE_API_KEY

def embed_cohere(texts: list[str], input_type: str = "search_document") -> list[list[float]]:
    # input_type: "search_document" for indexing, "search_query" for queries
    response = co.embed(
        texts=texts,
        model="embed-english-v3.0",   # 1024-dim
        input_type=input_type,
        embedding_types=["float"],
    )
    return response.embeddings.float
```

### Local sentence-transformers

```python
from sentence_transformers import SentenceTransformer

# BGE-large-en-v1.5: strong general-purpose, 1024-dim, runs on CPU
model = SentenceTransformer("BAAI/bge-large-en-v1.5")

def embed_local(texts: list[str], batch_size: int = 64) -> list[list[float]]:
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,  # required for cosine similarity via dot product
        show_progress_bar=True,
    )
    return embeddings.tolist()
```

**Embedding dimension guidance:**

| Model | Dims | Notes |
|-------|------|-------|
| text-embedding-3-small | 1536 | Best commercial value |
| text-embedding-3-large | 3072 | Higher quality, larger storage |
| embed-english-v3.0 (Cohere) | 1024 | Requires asymmetric query/doc types |
| BAAI/bge-large-en-v1.5 | 1024 | Best open-source general purpose |
| all-MiniLM-L6-v2 | 384 | Fast CPU inference, lower quality |

---

## Vector Stores

### pgvector (PostgreSQL)

Ideal for teams already running PostgreSQL. Supports HNSW and IVFFlat indexes.

```sql
-- Enable extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Document chunks table with metadata
CREATE TABLE document_chunks (
    chunk_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id       TEXT NOT NULL,
    source       TEXT NOT NULL,         -- file path or URL
    section      TEXT,                  -- h1/h2 header context
    chunk_index  INT NOT NULL,          -- position within document
    content_hash TEXT NOT NULL,         -- SHA-256 of chunk text for dedup
    content      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    doc_date     DATE,                  -- document publication date for recency filter
    embedding    vector(1536)           -- match embedding model dims
);

-- HNSW index for cosine similarity (best for normalized embeddings)
CREATE INDEX ON document_chunks
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200);

-- Composite index for filtered search — index on filter columns too
CREATE INDEX ON document_chunks (source, doc_date);
```

```python
import psycopg
import numpy as np

def upsert_chunks(conn_str: str, chunks: list[dict]) -> None:
    """chunks: list of {chunk_id, doc_id, source, section, chunk_index,
                         content_hash, content, embedding, doc_date}"""
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO document_chunks
                    (chunk_id, doc_id, source, section, chunk_index,
                     content_hash, content, embedding, doc_date)
                VALUES
                    (%(chunk_id)s, %(doc_id)s, %(source)s, %(section)s,
                     %(chunk_index)s, %(content_hash)s, %(content)s,
                     %(embedding)s::vector, %(doc_date)s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                    content      = EXCLUDED.content,
                    content_hash = EXCLUDED.content_hash,
                    embedding    = EXCLUDED.embedding,
                    section      = EXCLUDED.section,
                    doc_date     = EXCLUDED.doc_date
                """,
                chunks,
            )
        conn.commit()

def similarity_search(
    conn_str: str,
    query_embedding: list[float],
    source_filter: str | None = None,
    after_date: str | None = None,    # ISO date string
    top_k: int = 10,
) -> list[dict]:
    conditions = []
    params: dict = {"embedding": query_embedding, "k": top_k}
    if source_filter:
        conditions.append("source = %(source)s")
        params["source"] = source_filter
    if after_date:
        conditions.append("doc_date >= %(after_date)s::date")
        params["after_date"] = after_date

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
        SELECT chunk_id, doc_id, source, section, content,
               1 - (embedding <=> %(embedding)s::vector) AS cosine_score
        FROM document_chunks
        {where_clause}
        ORDER BY embedding <=> %(embedding)s::vector
        LIMIT %(k)s
    """
    with psycopg.connect(conn_str) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
```

**pgvector metadata filtering note:** From pgvector 0.8.0+, enable iterative HNSW scans for selective filters:
```sql
SET hnsw.iterative_scan = relaxed_order;
SET hnsw.ef_search = 100;  -- increase for more accurate but slower search
```

### Chroma (local development)

```python
import chromadb
from chromadb.config import Settings

# In-process (dev) or HTTP client (staging)
client = chromadb.PersistentClient(path="./chroma_data")
# client = chromadb.HttpClient(host="localhost", port=8000)

collection = client.get_or_create_collection(
    name="document_chunks",
    metadata={"hnsw:space": "cosine"},
)

# Upsert
collection.upsert(
    ids=[c["chunk_id"] for c in chunks],
    embeddings=[c["embedding"] for c in chunks],
    documents=[c["content"] for c in chunks],
    metadatas=[
        {"doc_id": c["doc_id"], "source": c["source"],
         "section": c["section"], "doc_date": c["doc_date"]}
        for c in chunks
    ],
)

# Search with metadata filter
results = collection.query(
    query_embeddings=[query_embedding],
    n_results=10,
    where={"source": {"$eq": "handbook.md"}},  # Chroma filter syntax
    include=["documents", "metadatas", "distances"],
)
```

### Qdrant (production)

```python
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue, Range,
)

client = QdrantClient(url="http://qdrant-host:6333", api_key="...")

# Create collection
client.recreate_collection(
    collection_name="document_chunks",
    vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
    # For named vectors (multi-embedding): use dict of VectorParams
)

# Upsert
points = [
    PointStruct(
        id=c["chunk_id"],   # must be UUID or uint64
        vector=c["embedding"],
        payload={
            "doc_id": c["doc_id"], "source": c["source"],
            "section": c["section"], "doc_date": c["doc_date"],
            "content": c["content"],
        },
    )
    for c in chunks
]
client.upsert(collection_name="document_chunks", points=points)

# Filtered similarity search
results = client.search(
    collection_name="document_chunks",
    query_vector=query_embedding,
    limit=10,
    query_filter=Filter(
        must=[
            FieldCondition(key="source", match=MatchValue(value="handbook.md")),
            FieldCondition(key="doc_date", range=Range(gte="2024-01-01")),
        ]
    ),
    with_payload=True,
)
```

### Weaviate

```python
import weaviate
import weaviate.classes as wvc

client = weaviate.connect_to_local()  # or connect_to_weaviate_cloud(...)

# Create collection (schema)
client.collections.create(
    name="DocumentChunk",
    vectorizer_config=wvc.config.Configure.Vectorizer.none(),  # bring-your-own vectors
    properties=[
        wvc.config.Property(name="doc_id", data_type=wvc.config.DataType.TEXT),
        wvc.config.Property(name="source", data_type=wvc.config.DataType.TEXT),
        wvc.config.Property(name="section", data_type=wvc.config.DataType.TEXT),
        wvc.config.Property(name="content", data_type=wvc.config.DataType.TEXT),
        wvc.config.Property(name="doc_date", data_type=wvc.config.DataType.DATE),
    ],
)

collection = client.collections.get("DocumentChunk")

# Batch upsert
with collection.batch.dynamic() as batch:
    for c in chunks:
        batch.add_object(
            properties={
                "doc_id": c["doc_id"], "source": c["source"],
                "section": c["section"], "content": c["content"],
                "doc_date": c["doc_date"],
            },
            vector=c["embedding"],
            uuid=c["chunk_id"],
        )

# Filtered vector search
response = collection.query.near_vector(
    near_vector=query_embedding,
    limit=10,
    filters=wvc.query.Filter.by_property("source").equal("handbook.md"),
    return_metadata=wvc.query.MetadataQuery(distance=True),
)
```

---

## Incremental Embedding Refresh Pipeline

### Change Detection

Use SHA-256 content hashes to detect which chunks actually changed. Avoid re-embedding unchanged content — can eliminate 60–80% of unnecessary API calls.

```python
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

def compute_content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

@dataclass
class ChunkRecord:
    chunk_id: str
    doc_id: str
    content_hash: str
    embedded_at: datetime

def get_existing_hashes(conn_str: str, doc_id: str) -> dict[str, ChunkRecord]:
    """Return {chunk_id: ChunkRecord} for all chunks of a document."""
    with psycopg.connect(conn_str) as conn:
        with conn.cursor(row_factory=psycopg.rows.class_row(ChunkRecord)) as cur:
            cur.execute(
                "SELECT chunk_id, doc_id, content_hash, created_at AS embedded_at "
                "FROM document_chunks WHERE doc_id = %s",
                (doc_id,),
            )
            return {r.chunk_id: r for r in cur.fetchall()}

def compute_diff(
    new_chunks: list[dict],
    existing: dict[str, ChunkRecord],
) -> tuple[list[dict], list[str]]:
    """Returns (chunks_to_upsert, chunk_ids_to_delete)."""
    new_ids = set()
    to_upsert = []
    for chunk in new_chunks:
        cid = chunk["chunk_id"]
        new_ids.add(cid)
        if cid not in existing or existing[cid].content_hash != chunk["content_hash"]:
            to_upsert.append(chunk)

    to_delete = [cid for cid in existing if cid not in new_ids]
    return to_upsert, to_delete

def delete_stale_chunks(conn_str: str, chunk_ids: list[str]) -> None:
    if not chunk_ids:
        return
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM document_chunks WHERE chunk_id = ANY(%s)",
                (chunk_ids,),
            )
        conn.commit()
```

### Airflow DAG for Incremental Refresh

```python
import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

@dag(
    dag_id="rag_embedding_refresh",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 2 * * *",   # nightly at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["rag", "embeddings"],
)
def rag_embedding_refresh():

    @task
    def list_changed_documents() -> list[str]:
        """Return doc_ids that changed since the last run (timestamp or hash check)."""
        from my_doc_store import get_documents_modified_since
        from my_db import get_last_run_timestamp

        last_run = get_last_run_timestamp(dag_id="rag_embedding_refresh")
        changed = get_documents_modified_since(last_run)
        return [d["doc_id"] for d in changed]

    @task
    def load_and_chunk(doc_id: str) -> list[dict]:
        from my_doc_store import load_document
        from my_chunker import chunk_document
        doc = load_document(doc_id)
        return chunk_document(doc)

    @task
    def embed_and_upsert(chunks: list[dict]) -> dict:
        from my_embedder import embed_batch_with_retry
        from my_vector_store import upsert_chunks, delete_stale_chunks, compute_diff, get_existing_hashes
        import os

        if not chunks:
            return {"upserted": 0, "deleted": 0}

        doc_id = chunks[0]["doc_id"]
        existing = get_existing_hashes(os.environ["PGVECTOR_CONN"], doc_id)
        to_upsert, to_delete = compute_diff(chunks, existing)

        if to_upsert:
            texts = [c["content"] for c in to_upsert]
            embeddings = embed_batch_with_retry(texts)
            for chunk, emb in zip(to_upsert, embeddings):
                chunk["embedding"] = emb
            upsert_chunks(os.environ["PGVECTOR_CONN"], to_upsert)

        delete_stale_chunks(os.environ["PGVECTOR_CONN"], to_delete)
        return {"upserted": len(to_upsert), "deleted": len(to_delete)}

    doc_ids = list_changed_documents()
    # Dynamic task mapping: one embed_and_upsert task per changed document
    chunks = load_and_chunk.expand(doc_id=doc_ids)
    embed_and_upsert.expand(chunks=chunks)

rag_embedding_refresh()
```

---

## Hybrid Retrieval

### Why Hybrid

Dense vector search excels at semantic similarity but misses exact token matches (error codes, product IDs, proper nouns). BM25 excels at keyword matching but misses paraphrase/synonym queries. Hybrid search combining both achieves ~91% recall@10 vs ~78% for either alone.

### Reciprocal Rank Fusion (RRF)

RRF is score-scale agnostic — it operates on ranks, not raw scores (cosine similarity and BM25 scores are incompatible scales). Formula: `score(d) = Σ 1 / (k + rank(d))` where k=60 is the standard constant.

```python
from rank_bm25 import BM25Okapi
from typing import Any

def rrf_fuse(
    dense_results: list[dict],   # [{chunk_id, content, ...}, ...]
    sparse_results: list[dict],  # same structure
    k: int = 60,
    dense_weight: float = 1.0,
    sparse_weight: float = 1.0,
) -> list[dict]:
    """Merge dense and sparse ranked lists with Reciprocal Rank Fusion."""
    scores: dict[str, float] = {}
    id_to_doc: dict[str, dict] = {}

    for rank, doc in enumerate(dense_results, start=1):
        cid = doc["chunk_id"]
        scores[cid] = scores.get(cid, 0.0) + dense_weight / (k + rank)
        id_to_doc[cid] = doc

    for rank, doc in enumerate(sparse_results, start=1):
        cid = doc["chunk_id"]
        scores[cid] = scores.get(cid, 0.0) + sparse_weight / (k + rank)
        id_to_doc[cid] = doc

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [id_to_doc[cid] for cid, _ in ranked]


class BM25Retriever:
    """In-memory BM25 retriever. For production, use Elasticsearch/OpenSearch."""

    def __init__(self, corpus: list[dict]):
        self.docs = corpus
        tokenized = [doc["content"].lower().split() for doc in corpus]
        self.bm25 = BM25Okapi(tokenized)

    def search(self, query: str, top_k: int = 20) -> list[dict]:
        tokens = query.lower().split()
        scores = self.bm25.get_scores(tokens)
        ranked_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
        return [self.docs[i] for i in ranked_idx[:top_k]]


def hybrid_search(
    query: str,
    query_embedding: list[float],
    bm25_retriever: BM25Retriever,
    vector_search_fn,   # callable(embedding, top_k) -> list[dict]
    top_k: int = 10,
    candidate_k: int = 50,
) -> list[dict]:
    dense = vector_search_fn(query_embedding, top_k=candidate_k)
    sparse = bm25_retriever.search(query, top_k=candidate_k)
    fused = rrf_fuse(dense, sparse)
    return fused[:top_k]
```

### Cross-Encoder Re-ranking

Reranking is the single most impactful component after retrieval — cross-encoders jointly encode query and document, providing fine-grained relevance scoring. MRR@3 typically jumps from ~0.43 to ~0.60.

```python
from sentence_transformers.cross_encoder import CrossEncoder

# Use a dedicated cross-encoder, not a bi-encoder
reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
# For production quality: "BAAI/bge-reranker-large" or Cohere Rerank API

def rerank(
    query: str,
    candidates: list[dict],
    top_n: int = 5,
) -> list[dict]:
    """Re-score and re-order candidates with a cross-encoder."""
    pairs = [(query, doc["content"]) for doc in candidates]
    scores = reranker.predict(pairs, batch_size=32)
    ranked = sorted(
        zip(candidates, scores), key=lambda x: x[1], reverse=True
    )
    return [doc for doc, _ in ranked[:top_n]]


# Full two-stage retrieval
def retrieve_for_rag(
    query: str,
    query_embedding: list[float],
    bm25_retriever: BM25Retriever,
    vector_search_fn,
    top_k_final: int = 5,
) -> list[dict]:
    # Stage 1: hybrid retrieval, broad candidate pool
    candidates = hybrid_search(
        query, query_embedding, bm25_retriever, vector_search_fn,
        top_k=100, candidate_k=50,
    )
    # Stage 2: cross-encoder reranking, narrow to final context
    return rerank(query, candidates, top_n=top_k_final)
```

---

## Metadata Filtering

Attach structured metadata to every chunk at index time. Use it to scope retrieval to the right subset before vector scoring.

**Common metadata fields:**

| Field | Type | Use Case |
|-------|------|----------|
| `doc_id` | str/UUID | Delete all chunks of a doc |
| `source` | str | Filter by document path / URL |
| `section` | str | Filter by chapter / heading |
| `doc_date` | date | Recency constraint (only docs after X) |
| `content_hash` | str | Deduplication and change detection |
| `language` | str | Multilingual collections |
| `access_level` | str | Security-scoped retrieval |

**pgvector filter example:**
```sql
-- Retrieve only from specific source and last 90 days
SELECT chunk_id, content,
       1 - (embedding <=> $1::vector) AS score
FROM document_chunks
WHERE source = 'internal_handbook'
  AND doc_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY embedding <=> $1::vector
LIMIT 10;
```

**Qdrant filter example:**
```python
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

query_filter = Filter(
    must=[
        FieldCondition(key="access_level", match=MatchValue(value="public")),
    ],
    should=[
        FieldCondition(key="section", match=MatchValue(value="pricing")),
        FieldCondition(key="section", match=MatchValue(value="billing")),
    ],
)
results = client.search(
    collection_name="document_chunks",
    query_vector=query_embedding,
    query_filter=query_filter,
    limit=10,
)
```

---

## Production Considerations

### Deduplication of Chunks

```python
def deduplicate_chunks(chunks: list[dict]) -> list[dict]:
    """Remove exact-duplicate chunks by content hash before embedding."""
    seen: set[str] = set()
    unique = []
    for chunk in chunks:
        h = compute_content_hash(chunk["content"])
        chunk["content_hash"] = h
        if h not in seen:
            seen.add(h)
            unique.append(chunk)
    return unique
```

Near-duplicate detection: use MinHash / SimHash for fuzzy dedup when slight reformatting creates near-duplicates across documents.

### Monitoring Retrieval Quality

Track these metrics in production:

```python
from dataclasses import dataclass, field
from statistics import mean

@dataclass
class RetrievalMetrics:
    hit_rate: list[float] = field(default_factory=list)   # 1 if answer in top-k
    mrr: list[float] = field(default_factory=list)         # mean reciprocal rank
    latency_ms: list[float] = field(default_factory=list)

    def record_query(
        self,
        ranked_chunks: list[dict],
        relevant_chunk_ids: set[str],
        latency_ms: float,
    ) -> None:
        retrieved_ids = [c["chunk_id"] for c in ranked_chunks]
        hit = any(cid in relevant_chunk_ids for cid in retrieved_ids)
        self.hit_rate.append(float(hit))

        rr = 0.0
        for rank, cid in enumerate(retrieved_ids, start=1):
            if cid in relevant_chunk_ids:
                rr = 1.0 / rank
                break
        self.mrr.append(rr)
        self.latency_ms.append(latency_ms)

    def summary(self) -> dict:
        return {
            "hit_rate": round(mean(self.hit_rate), 4) if self.hit_rate else 0,
            "mrr": round(mean(self.mrr), 4) if self.mrr else 0,
            "p95_latency_ms": sorted(self.latency_ms)[int(len(self.latency_ms) * 0.95)]
                              if self.latency_ms else 0,
            "n_queries": len(self.hit_rate),
        }
```

### A/B Testing Retrieval Strategies

```python
import random

def route_retrieval_strategy(query: str, user_id: str) -> str:
    """Deterministic A/B split based on user_id hash."""
    bucket = int(hashlib.md5(user_id.encode()).hexdigest(), 16) % 100
    if bucket < 50:
        return "dense_only"        # control
    else:
        return "hybrid_rerank"     # treatment

# Log each query with strategy for offline analysis
experiment_log = {
    "query_id": query_id,
    "strategy": strategy,
    "user_id": user_id,
    "retrieved_chunk_ids": [c["chunk_id"] for c in results],
    "user_feedback": None,   # filled in via thumbs up/down
}
```

### Latency vs Quality Tradeoffs

| Component | Latency | Quality | When to Add |
|-----------|---------|---------|-------------|
| Dense-only retrieval | low (5–20ms) | medium | Dev / low-stakes |
| + BM25 hybrid + RRF | medium (+10–30ms) | high | Production default |
| + Cross-encoder rerank | high (+50–200ms) | very high | High-value queries, async flows |
| Semantic chunking | offline cost | better precision | Narrative documents |
| Matryoshka dims (256) | low (index 6x smaller) | slightly lower | High-scale, cost-sensitive |

**Key thresholds (production baselines):**
- Hit rate (answer in top-10): target >85% on evaluation set
- MRR@5: target >0.60 after reranking
- p95 online latency: <500ms end-to-end including LLM call
- Embedding refresh lag: <24h for mutable knowledge bases

### Embedding Model Versioning

When you upgrade the embedding model, ALL existing vectors become stale (different embedding space).

```python
# Store embedding_model_version in chunk metadata
chunk["embedding_model"] = "text-embedding-3-small-v1"

# Query: only search within same model version
results = similarity_search(
    conn_str=CONN,
    query_embedding=embed(query, model="text-embedding-3-small"),
    model_version_filter="text-embedding-3-small-v1",
)
```

Run a background re-indexing job when upgrading models; maintain a `model_version` column and index; cut over queries atomically after the job completes.

---

## Anti-Patterns

- **Chunk size too large (>2000 chars):** Dilutes the embedding signal; the model must represent too many topics in one vector. Use hierarchical chunking (section-level + chunk-level) instead.
- **No chunk overlap:** Sentences at boundaries lose context. Always use 10–15% overlap.
- **Re-embedding all documents on every run:** Compute content hashes and only re-embed changed chunks. Re-embedding is expensive and causes unnecessary API costs.
- **Skipping deduplication:** Near-duplicate chunks from repeated boilerplate (headers, footers, disclaimers) will saturate the context window with redundant text. Deduplicate by hash before indexing.
- **Dense-only retrieval for technical queries:** Exact token matches (error codes, CLI flags, product SKUs) require BM25. Always use hybrid for technical knowledge bases.
- **RRF score normalization:** Do not normalize BM25 and cosine scores before fusion — incompatible scales cause unpredictable results. Use RRF on ranks instead.
- **Cross-encoder on full corpus:** Cross-encoders are O(n) per query. Only apply them to the top-100 candidates from the first-stage retrieval.
- **Ignoring filter selectivity with pgvector:** Highly selective metadata filters + HNSW can return fewer results than `LIMIT`. Enable `hnsw.iterative_scan = relaxed_order` for production filtered search.
- **Missing embedding model versioning:** Upgrading the embedding model without re-indexing all chunks produces mixed embedding spaces. Results become unpredictably wrong for unchanged documents.
- **No retrieval quality monitoring:** Without hit-rate and MRR tracking against a labelled evaluation set, chunking and retrieval regressions go undetected after changes.

---

## References to Consult When Needed

- pgvector HNSW tuning: https://github.com/pgvector/pgvector — `ef_construction`, `m`, `iterative_scan`
- Qdrant filtering docs: https://qdrant.tech/documentation/concepts/filtering/
- LangChain text splitters: https://python.langchain.com/docs/modules/data_connection/document_transformers/
- OpenAI embeddings: https://platform.openai.com/docs/guides/embeddings
- Cohere Rerank API: https://docs.cohere.com/reference/rerank
- sentence-transformers cross-encoders: https://www.sbert.net/docs/pretrained_cross-encoders.html
- BM25 Python: https://github.com/dorianbrown/rank_bm25
- RRF algorithm: Cormack, Clarke, Buettcher (2009) — "Reciprocal Rank Fusion outperforms Condorcet and individual rank learning methods"
