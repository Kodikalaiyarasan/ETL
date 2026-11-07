#!/usr/bin/env python3
"""
Unit Tests for E-Commerce Data Lakehouse — Elasticsearch Pipeline
===================================================================
Tests index mappings, review enrichment (sentiment & defaults),
and bulk action generation.
"""

import pytest
from search.index_elasticsearch import (
    enrich_review,
    generate_actions,
    REVIEWS_INDEX_SETTINGS,
    CLICKSTREAM_INDEX_SETTINGS,
    REVIEWS_INDEX,
    CLICKSTREAM_INDEX,
)


class TestElasticsearchPipeline:
    """Test suite for Elasticsearch schemas and indexing logic."""

    def test_review_index_mapping_structure(self):
        """Verify reviews index settings have required analyzers and mappings."""
        props = REVIEWS_INDEX_SETTINGS["mappings"]["properties"]
        assert "review_id" in props
        assert props["review_id"]["type"] == "keyword"
        assert "review_text" in props
        assert props["review_text"]["type"] == "text"
        assert props["review_text"]["analyzer"] == "review_analyzer"
        assert "sentiment" in props
        assert props["sentiment"]["type"] == "keyword"

    def test_clickstream_index_mapping_structure(self):
        """Verify clickstream index mapping includes IP type and timestamp."""
        props = CLICKSTREAM_INDEX_SETTINGS["mappings"]["properties"]
        assert "event_id" in props
        assert props["event_id"]["type"] == "keyword"
        assert "ip_address" in props
        assert props["ip_address"]["type"] == "ip"
        assert "event_timestamp" in props
        assert props["event_timestamp"]["type"] == "date"

    def test_enrich_review_positive_sentiment(self):
        """Verify 4 & 5 star ratings get mapped to positive sentiment."""
        raw = {
            "review_id": "REV-000001",
            "product_id": "PROD-00001",
            "rating": 5,
            "review_text": "Superb product!",
        }
        enriched = enrich_review(raw)
        assert enriched["sentiment"] == "positive"
        assert enriched["rating"] == 5

    def test_enrich_review_negative_sentiment(self):
        """Verify 1 & 2 star ratings get mapped to negative sentiment."""
        raw = {
            "review_id": "REV-000002",
            "product_id": "PROD-00002",
            "rating": 1,
            "review_text": "Broke immediately.",
        }
        enriched = enrich_review(raw)
        assert enriched["sentiment"] == "negative"

    def test_enrich_review_neutral_sentiment(self):
        """Verify 3 star ratings get mapped to neutral sentiment."""
        raw = {
            "review_id": "REV-000003",
            "product_id": "PROD-00003",
            "rating": 3,
            "review_text": "Average item.",
        }
        enriched = enrich_review(raw)
        assert enriched["sentiment"] == "neutral"

    def test_enrich_review_empty_text_default(self):
        """Verify empty review text gets replaced with default fallback."""
        raw = {
            "review_id": "REV-000004",
            "product_id": "PROD-00004",
            "rating": 4,
            "review_text": "",
        }
        enriched = enrich_review(raw)
        assert enriched["review_text"] == "No review text provided."

    def test_generate_actions(self):
        """Verify bulk actions generator sets index and doc_id correctly."""
        records = [
            {"review_id": "REV-101", "rating": 5},
            {"review_id": "REV-102", "rating": 4},
        ]
        actions = list(generate_actions(records, REVIEWS_INDEX, "review_id"))
        assert len(actions) == 2
        assert actions[0]["_index"] == REVIEWS_INDEX
        assert actions[0]["_id"] == "REV-101"
        assert actions[0]["_source"]["rating"] == 5

