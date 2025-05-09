# 📍 GSheetsEtl — Address Geocoding ETL Pipeline

## Overview

**GSheetsEtl** is a Python-based ETL (Extract, Transform, Load) class designed to handle address data collected from a Google Form via a published Google Sheets URL. The pipeline:

1. **Extracts** addresses from a published CSV.
2. **Geocodes** the addresses using the U.S. Census Geocoding API.
3. **Loads** the resulting points into a GIS as a point feature class.

The class extends from a base `SpatialEtl` class and uses libraries like `requests`, `csv`, and `arcpy`.

---

## 📦 Features

- Download address data from a live Google Sheets link.
- Automatically geocode addresses using the U.S. Census Bureau API.
- Create an `avoid_points` point feature class in ArcGIS based on geocoded locations.
- Modular ETL workflow: `extract()`, `transform()`, `load()`, and `process()`.

---

## 🗺️ Workflow Diagram

```mermaid
graph TD;
    A[Extract from Google Sheets CSV] --> B[Transform: Geocode Addresses];
    B --> C[Save Geocoded Results as CSV];
    C --> D[Load into ArcGIS Feature Class];
